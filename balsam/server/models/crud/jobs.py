from collections import defaultdict
from datetime import datetime
from itertools import chain
from logging import getLogger
from typing import Any, Dict, Iterable, List, Optional, Tuple

from fastapi import HTTPException, status
from sqlalchemy import Column, bindparam, func, insert, orm, select, update
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
    stmt = paginator.paginate_core(stmt).order_by(models.Job.id)
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


def _update_state(
    job: models.Job, state: str, state_timestamp: datetime, state_data: Dict[str, Any], transfer_items: List[Any]
) -> Tuple[Dict[str, Any], Dict[str, Any], List[int]]:
    if state == job.state or state is None:
        return {}, {}, []

    event = dict(
        job_id=job.id,
        from_state=job.state,
        to_state=state,
        timestamp=state_timestamp,
        data=state_data,
    )
    assert isinstance(event["data"], dict)

    update_dict: Dict[str, Any] = {"job_id": job.id, "state": state, "session_id": job.session_id}
    ready_transfers = []

    if state != "RUNNING" and job.session_id is not None:
        update_dict["session_id"] = None

    if state == "READY":
        if all(item.state == "done" for item in transfer_items if item.direction == "in"):
            update_dict["state"] = "STAGED_IN"
            event["data"]["message"] = "Skipped stage in"

    if state == "POSTPROCESSED":
        if all(item.state == "done" for item in transfer_items if item.direction == "out"):
            update_dict["state"] = "JOB_FINISHED"
            event["data"]["message"] = "Skipped stage out"
        else:
            for item in transfer_items:
                if item.state == "awaiting_job":
                    ready_transfers.append(item.id)

    if state == "STAGED_OUT":
        update_dict["state"] = "JOB_FINISHED"

    event["to_state"] = update_dict["state"]
    return update_dict, event, ready_transfers


def update_waiting_children(db: Session, finished_parent_ids: Iterable[int]) -> None:
    """
    When all of a job's parents reach JOB_FINISHED, update the job to READY
    """
    if not finished_parent_ids:
        return

    qs = (
        select((models.Job.__table__,))
        .where(models.Job.state == "AWAITING_PARENTS")
        .where(models.Job.parent_ids.overlap(finished_parent_ids))  # type: ignore[attr-defined]
    )
    children, transfer_items_by_jobid = select_jobs_for_update(db, qs, extra_cols=[models.Job.parent_ids])

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
    state_updates: List[Dict[str, Any]] = []
    events: List[Dict[str, Any]] = []

    for child in ready_children:
        state_update, event, _ = _update_state(
            job=child,
            state="READY",
            state_timestamp=now,
            state_data={"message": "All parents finished"},
            transfer_items=transfer_items_by_jobid[child.id],
        )
        if state_update:
            state_updates.append(state_update)
            events.append(event)

    if state_updates:
        db.execute(
            update(models.Job.__table__).where(models.Job.id == bindparam("job_id")),
            state_updates,
        )
        db.execute(insert(models.LogEvent.__table__), events)


def select_jobs_for_update(
    db: Session, qs: "Select", extra_cols: Optional[List[Column[Any]]] = None
) -> Tuple[List[Any], Dict[int, Any]]:
    """
    Select Jobs FOR UPDATE
    Return List[(job.id, job.state, job.session_id]) and Dict[job_id, (transfer.state, transfer.direction)]
    """
    if extra_cols is None:
        extra_cols = []
    qs = qs.with_only_columns([models.Job.id, models.Job.state, models.Job.session_id, *extra_cols])
    qs = qs.with_for_update(of=models.Job.__table__)
    jobs: List[Any] = db.execute(qs).all()
    job_ids = [job.id for job in jobs]

    transfer_items_by_job = defaultdict(list)
    for item in db.execute(
        select(
            (
                models.TransferItem.id,
                models.TransferItem.job_id,
                models.TransferItem.state,
                models.TransferItem.direction,
            )
        ).where(models.TransferItem.job_id.in_(job_ids))
    ):
        transfer_items_by_job[item.job_id].append(item)
    return jobs, transfer_items_by_job


def do_update_jobs(
    db: Session,
    update_jobs: List[Any],
    transfer_items_by_jobid: Dict[int, List[Any]],
    patch_dicts: Dict[int, Dict[str, Any]],
) -> None:
    events: List[Dict[str, Any]] = []
    ready_transfers: List[int] = []

    # First, perform job-wise updates:
    for job in update_jobs:
        update_data = patch_dicts[job.id]
        update_data["job_id"] = job.id
        if "workdir" in update_data:
            update_data["workdir"] = str(update_data["workdir"])

        state_update, event, update_transfer_ids = _update_state(
            job=job,
            state=update_data.pop("state", None),
            state_timestamp=update_data.pop("state_timestamp", datetime.utcnow()),
            state_data=update_data.pop("state_data", {}),
            transfer_items=transfer_items_by_jobid[job.id],
        )
        if state_update:
            update_data.update(state_update)
            events.append(event)
            ready_transfers.extend(update_transfer_ids)

    updates_list = list(patch_dicts.values())
    if updates_list:
        db.execute(
            update(models.Job.__table__).where(models.Job.id == bindparam("job_id")),
            updates_list,
        )
    if events:
        db.execute(insert(models.LogEvent.__table__), events)
    if ready_transfers:
        db.execute(
            update(models.TransferItem.__table__)
            .where(models.TransferItem.id.in_(ready_transfers))
            .values(state="pending")
        )

    # Then update affected children in a second query:
    finished_ids = [patch["job_id"] for patch in patch_dicts.values() if patch.get("state") == "JOB_FINISHED"]
    update_waiting_children(db, finished_ids)


def bulk_update(db: Session, owner: schemas.UserOut, patch_dicts: Dict[int, Dict[str, Any]]) -> int:
    job_ids = set(patch_dicts.keys())
    qs = owned_job_selector(owner).where(models.Job.id.in_(job_ids))
    update_jobs, transfer_items_by_jobid = select_jobs_for_update(db, qs)
    if len(update_jobs) < len(patch_dicts):
        raise ValidationError("Could not find some Job IDs")
    do_update_jobs(db, update_jobs, transfer_items_by_jobid, patch_dicts)
    return len(update_jobs)


def update_query(db: Session, owner: schemas.UserOut, update_data: Dict[str, Any], filterset: JobQuery) -> int:
    qs = owned_job_selector(owner)
    qs = filterset.apply_filters(qs)
    update_jobs, transfer_items_by_jobid = select_jobs_for_update(db, qs)
    if len(update_jobs) > schemas.MAX_ITEMS_PER_BULK_OP:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot bulk-update more than {schemas.MAX_ITEMS_PER_BULK_OP} in a single API call.",
        )
    patch_dicts = {job.id: update_data.copy() for job in update_jobs}
    do_update_jobs(db, update_jobs, transfer_items_by_jobid, patch_dicts)
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
