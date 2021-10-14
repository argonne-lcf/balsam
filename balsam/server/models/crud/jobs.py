from datetime import datetime
from itertools import chain
from logging import getLogger
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from fastapi import HTTPException, status
from sqlalchemy import Column, func, insert, orm, select
from sqlalchemy.orm import Query, Session
from sqlalchemy.sql import Select

from balsam import schemas
from balsam.schemas.job import JobState, JobTransferItem
from balsam.server import ValidationError, models
from balsam.server.routers.filters import JobQuery
from balsam.server.utils import Paginator

logger = getLogger(__name__)


def owned_job_selector(owner: schemas.UserOut, columns: Optional[List[Column[Any]]] = None) -> Select:
    if columns is None:
        stmt = select(models.Job.__table__)  # type: ignore[arg-type]
    else:
        stmt = select(columns)
    return (  # type: ignore
        stmt.join(models.App.__table__, models.Job.app_id == models.App.id)  # type: ignore
        .join(models.Site.__table__, models.App.site_id == models.Site.id)
        .where(models.Site.owner_id == owner.id)
    )


def owned_job_query(db: Session, owner: schemas.UserOut, id_only: bool = False) -> "Query[models.Job]":
    qs: "Query[models.Job]" = db.query(models.Job.id) if id_only else db.query(models.Job)
    qs = qs.join(models.App).join(models.Site).filter(models.Site.owner_id == owner.id)  # type: ignore
    return qs


def update_states_by_query(
    qs: "Query[models.Job]",
    state: str,
    data: Union[Dict[str, Any], str, None] = None,
) -> int:
    now = datetime.utcnow()
    if isinstance(data, str):
        data = {"message": data}
    elif data is None:
        data = {}

    updated_jobs = _select_jobs_for_update(qs)
    for job in updated_jobs:
        _update_state(job, state, now, data)
    return len(updated_jobs)


def populate_transfers(app: models.App, transfers: Dict[str, JobTransferItem]) -> List[Dict[str, Any]]:
    transfer_items = []
    for transfer_name, transfer_spec in transfers.items():
        transfer_slot = app.transfers.get(transfer_name)
        if transfer_slot is None:
            raise ValidationError(f"App {app.name} has no Transfer slot named {transfer_name}")
        direction, local_path = transfer_slot["direction"], transfer_slot["local_path"]
        recursive = transfer_slot["recursive"]
        assert direction in ["in", "out"]
        location_alias, remote_path = (
            transfer_spec.location_alias,
            transfer_spec.path.as_posix(),
        )
        url = app.site.transfer_locations.get(location_alias)
        if url is None:
            raise ValidationError(f"Site has no Transfer URL named {location_alias}")

        transfer_item = dict(
            direction=direction,
            local_path=local_path,
            remote_path=remote_path,
            recursive=recursive,
            location_alias=location_alias,
            state="pending" if direction == "in" else "awaiting_job",
            task_id="",
            transfer_info={},
        )
        transfer_items.append(transfer_item)
    return transfer_items


def fetch(
    db: Session,
    owner: schemas.UserOut,
    paginator: Optional[Paginator[models.Job]] = None,
    job_id: Optional[int] = None,
    filterset: Optional[JobQuery] = None,
) -> "Tuple[int, List[Dict[str, Any]]]":
    stmt = owned_job_selector(owner)
    if job_id is not None:
        stmt = stmt.where(models.Job.id == job_id)
    if filterset:
        stmt = filterset.apply_filters(stmt)
    if paginator is None:
        job = db.execute(stmt).mappings().one()
        return 1, [dict(job)]
    count_q = stmt.with_only_columns([func.count(models.Job.id)]).order_by(None)
    count = db.execute(count_q).scalar()
    stmt = paginator.paginate_core(stmt)
    job_rows = [dict(j) for j in db.execute(stmt).mappings()]
    return count, job_rows


def bulk_create(
    db: Session, owner: schemas.UserOut, job_specs: List[schemas.ServerJobCreate]
) -> List[Dict[str, Any]]:
    now = datetime.utcnow()

    app_ids = set(job.app_id for job in job_specs)
    apps: Dict[int, models.App] = {
        app.id: app
        for app in db.query(models.App)
        .join(models.App.site)  # type: ignore
        .filter(models.Site.owner_id == owner.id, models.App.id.in_(app_ids))
        .options(orm.selectinload(models.App.site).load_only(models.Site.transfer_locations))
        .all()
    }
    if len(apps) < len(app_ids):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Could not find one or more Apps. Double check app_id fields.",
        )

    parent_ids = set(pid for job in job_specs for pid in job.parent_ids)
    if parent_ids:
        parent_selector = owned_job_selector(owner, columns=[models.Job.id, models.Job.state])
        parent_selector = parent_selector.where(models.Job.id.in_(parent_ids))
        parent_states_by_id = {parent.id: parent.state for parent in db.execute(parent_selector)}
    else:
        parent_states_by_id = {}

    if len(parent_states_by_id) < len(parent_ids):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Could not find one or more Jobs set as parents. Double check parent_ids fields.",
        )

    workdirs = set(job.workdir for job in job_specs)
    if len(workdirs) < len(job_specs):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Workdirs must be unique for each call to bulk-create Jobs.",
        )

    JobDict = Dict[str, Any]
    TransferList = List[Dict[str, Any]]
    jobs_by_workdir: Dict[str, JobDict] = {}
    transfer_lists_by_workdir: Dict[str, TransferList] = {}

    # Initial Job States depend on parents and any stage-in transfer items
    defaults = {
        "last_update": now,
        "pending_file_cleanup": True,
        "serialized_return_value": "",
        "serialized_exception": "",
        "batch_job_id": None,
    }
    for job_spec in job_specs:
        job_transfers = populate_transfers(apps[job_spec.app_id], job_spec.transfers)

        if any(parent_states_by_id[pid] != JobState.job_finished for pid in job_spec.parent_ids):
            state = JobState.awaiting_parents
        elif any(tr["direction"] == "in" for tr in job_transfers):
            state = JobState.ready
        else:
            state = JobState.staged_in

        # TODO: Removed jsonable_encoder for performance; what's needed to make the job_dict
        # SQLAlchemy safe?
        job_dict = job_spec.dict(exclude={"transfers", "workdir", "parent_ids"})
        workdir = job_spec.workdir.as_posix()
        job_dict.update(
            **defaults,
            state=state,
            workdir=workdir,
            parent_ids=list(job_spec.parent_ids),
        )

        jobs_by_workdir[workdir] = job_dict
        transfer_lists_by_workdir[workdir] = job_transfers

    # Bulk create jobs, RETURNING ids array and setting ids on job dicts
    stmt = insert(models.Job.__table__).returning(models.Job.workdir, models.Job.id)
    for res in db.execute(stmt, list(jobs_by_workdir.values())):
        jobs_by_workdir[res.workdir]["id"] = res.id

        for transfer_dict in transfer_lists_by_workdir[res.workdir]:
            transfer_dict["job_id"] = res.id

    # Bulk create transfer items
    transfers_flat_list = list(chain(*transfer_lists_by_workdir.values()))
    if transfers_flat_list:
        db.execute(insert(models.TransferItem.__table__), transfers_flat_list)

    # Bulk create events
    events = [
        dict(
            job_id=job["id"],
            timestamp=now,
            from_state=JobState.created,
            to_state=job["state"],
            data={},
        )
        for job in jobs_by_workdir.values()
    ]
    db.execute(insert(models.LogEvent.__table__), events)

    logger.debug(f"Bulk-created {len(jobs_by_workdir)} jobs")
    return list(jobs_by_workdir.values())


def _update_state(job: models.Job, state: str, state_timestamp: datetime, state_data: Dict[str, Any]) -> None:
    if state == job.state or state is None:
        return None
    event = models.LogEvent(
        job=job,
        from_state=job.state,
        to_state=state,
        timestamp=state_timestamp,
        data=state_data,
    )
    assert isinstance(event.data, dict)
    job.state = state

    if job.state != "RUNNING" and job.session_id is not None:
        job.session = None
        job.session_id = None

    if job.state == "READY":
        if all(item.state == "done" for item in job.transfer_items if item.direction == "in"):
            job.state = "STAGED_IN"
            event.data["message"] = "Skipped stage in"

    if job.state == "POSTPROCESSED":
        if all(item.state == "done" for item in job.transfer_items if item.direction == "out"):
            job.state = "JOB_FINISHED"
            event.data["message"] = "Skipped stage out"
        else:
            for item in job.transfer_items:
                if item.state == "awaiting_job":
                    item.state = "pending"

    if job.state == "STAGED_OUT":
        job.state = "JOB_FINISHED"

    event.to_state = job.state


def _update(job: models.Job, data: Dict[str, Any]) -> None:
    state = data.pop("state", None)
    state_timestamp = data.pop("state_timestamp", datetime.utcnow())
    state_data = data.pop("state_data", {})

    workdir = data.pop("workdir", None)
    if workdir:
        job.workdir = str(workdir)
    for k, v in data.items():
        setattr(job, k, v)
    if data.get("return_code") is not None:
        job.return_code = data.get("return_code")
    if data.get("data") is not None:
        job.data = data.get("data")
    _update_state(job, state, state_timestamp, state_data)


def _check_waiting_children(db: Session, finished_parent_ids: Iterable[int]) -> None:
    """
    When all of a job's parents reach JOB_FINISHED, update the job to READY
    """
    if not finished_parent_ids:
        return

    children: List[models.Job] = (
        db.query(models.Job)
        .options(  # type: ignore
            orm.load_only(models.Job.id, models.Job.state, models.Job.parent_ids, models.Job.session_id),
            orm.selectinload(models.Job.transfer_items).load_only(
                models.TransferItem.state,
                models.TransferItem.direction,
            ),
        )
        .filter(models.Job.state == "AWAITING_PARENTS")
        .filter(models.Job.parent_ids.overlap(finished_parent_ids))  # type: ignore[attr-defined]
        .with_for_update(of=models.Job)
        .all()
    )
    if not children:
        return

    parent_states = {pid: "JOB_FINISHED" for pid in finished_parent_ids}

    all_parent_ids = set(pid for job in children for pid in job.parent_ids)
    parent_ids_tofetch = all_parent_ids - set(finished_parent_ids)

    if parent_ids_tofetch:
        for job in db.execute(select((models.Job.id, models.Job.state)).where(models.Job.id.in_(parent_ids_tofetch))):
            parent_states[job.id] = job.state

    ready_children = []
    for child in children:
        if all(parent_states.get(pid) == "JOB_FINISHED" for pid in child.parent_ids):
            ready_children.append(child)

    now = datetime.utcnow()
    for child in ready_children:
        _update_state(child, "READY", now, {"message": "All parents finished"})


def _select_jobs_for_update(qs: "Query[models.Job]") -> List[models.Job]:
    qs = qs.options(  # type: ignore
        orm.load_only(models.Job.id, models.Job.state, models.Job.session_id),
        orm.selectinload(models.Job.transfer_items).load_only(
            models.TransferItem.state,
            models.TransferItem.direction,
        ),
    ).with_for_update(of=models.Job)
    return qs.all()


def _update_jobs(db: Session, update_jobs: List[models.Job], patch_dicts: Dict[int, Dict[str, Any]]) -> None:
    # First, perform job-wise updates:
    for job in update_jobs:
        _update(job, patch_dicts[job.id])
    db.flush()

    # Then update affected children in a second query:
    finished_ids = [job.id for job in update_jobs if job.state == "JOB_FINISHED"]
    _check_waiting_children(db, finished_ids)
    db.flush()


def bulk_update(db: Session, owner: schemas.UserOut, patch_dicts: Dict[int, Dict[str, Any]]) -> int:
    job_ids = set(patch_dicts.keys())
    qs = owned_job_query(db, owner).filter(models.Job.id.in_(job_ids))
    update_jobs = _select_jobs_for_update(qs)
    if len(update_jobs) < len(patch_dicts):
        raise ValidationError("Could not find some Job IDs")
    _update_jobs(db, update_jobs, patch_dicts)
    return len(update_jobs)


def update_query(db: Session, owner: schemas.UserOut, update_data: Dict[str, Any], filterset: JobQuery) -> int:
    qs = owned_job_query(db, owner)
    qs = filterset.apply_filters(qs)
    update_jobs = _select_jobs_for_update(qs)
    patch_dicts = {job.id: update_data.copy() for job in update_jobs}
    _update_jobs(db, update_jobs, patch_dicts)
    return len(update_jobs)


def delete_query(
    db: Session,
    owner: schemas.UserOut,
    filterset: Optional[JobQuery] = None,
    job_id: Optional[int] = None,
) -> int:
    qs = owned_job_query(db, owner, id_only=True)
    if job_id is not None:
        qs = qs.filter(models.Job.id == job_id)
        qs.one()
    else:
        assert filterset is not None
        qs = filterset.apply_filters(qs)
    qs = qs.filter(models.Job.session_id.is_(None)).with_for_update(of=models.Job, skip_locked=True)  # type: ignore
    num_deleted = db.query(models.Job).filter(models.Job.id.in_(qs)).delete(synchronize_session=False)
    db.flush()
    logger.debug(f"Deleted {num_deleted} jobs")
    return num_deleted
