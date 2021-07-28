from datetime import datetime
from typing import Iterable, List, Optional, Sequence, Tuple, cast

from sqlalchemy import orm
from sqlalchemy.orm import Query, Session

from balsam import schemas
from balsam.server import ValidationError, models
from balsam.server.routers.filters import TransferItemQuery
from balsam.server.utils import Paginator


def owned_transfer_query(db: Session, owner: schemas.UserOut) -> "Query[models.TransferItem]":
    return cast(
        "Query[models.TransferItem]",
        db.query(models.TransferItem)
        .join(models.Job)  # type: ignore
        .join(models.App)
        .join(models.Site)
        .filter(models.Site.owner_id == owner.id),
    )


def fetch(
    db: Session,
    owner: schemas.UserOut,
    paginator: Optional[Paginator[models.TransferItem]] = None,
    transfer_id: Optional[int] = None,
    filterset: Optional[TransferItemQuery] = None,
) -> Tuple[int, Iterable[models.TransferItem]]:
    qs = owned_transfer_query(db, owner)
    if transfer_id is not None:
        qs = qs.filter(models.TransferItem.id == transfer_id)
        transfer_item = qs.one()
        return 1, [transfer_item]
    if filterset is not None:
        qs = filterset.apply_filters(qs)
    count = qs.group_by(models.TransferItem.id).count()
    assert paginator is not None
    transfers = paginator.paginate(qs)
    return count, transfers


def _set_transfer_state(job: models.Job) -> bool:
    direction = "in" if job.state == "READY" else "out"
    is_done = all(item.state == "done" for item in job.transfer_items if item.direction == direction)
    if is_done:
        job.state = "STAGED_IN" if direction == "in" else "JOB_FINISHED"
    return is_done


def _update_jobs(db: Session, job_ids: Sequence[int]) -> Tuple[List[models.Job], List[models.LogEvent]]:
    jobs = (
        db.query(models.Job)
        .filter(models.Job.id.in_(job_ids), models.Job.state.in_(["READY", "POSTPROCESSED"]))
        .options(  # type: ignore
            orm.selectinload(models.Job.transfer_items).load_only(
                models.TransferItem.direction,
                models.TransferItem.state,
            ),
            orm.selectinload(models.Job.parents).load_only(models.Job.id),
        )
        .all()
    )
    updates, events = [], []
    now = datetime.utcnow()
    for job in jobs:
        job.parent_ids = [parent.id for parent in job.parents]
        old_state = job.state
        if _set_transfer_state(job):
            event = models.LogEvent(
                job=job,
                timestamp=now,
                from_state=old_state,
                to_state=job.state,
            )
            updates.append(job)
            events.append(event)
    db.flush()
    return updates, events


def update(
    db: Session, owner: schemas.UserOut, transfer_id: int, data: schemas.TransferItemUpdate
) -> Tuple[models.TransferItem, Optional[models.Job], Optional[models.LogEvent]]:
    item = owned_transfer_query(db, owner).filter(models.TransferItem.id == transfer_id).one()
    for k, v in data.dict(exclude_unset=True).items():
        setattr(item, k, v)
    db.flush()
    updated_jobs, events = _update_jobs(db, [item.job_id])
    updated_job = updated_jobs[0] if updated_jobs else None
    event = events[0] if events else None
    return item, updated_job, event


def bulk_update(
    db: Session, owner: schemas.UserOut, update_list: List[schemas.TransferItemBulkUpdate]
) -> Tuple[List[models.TransferItem], List[models.Job], List[models.LogEvent]]:
    updates = {item.id: item.dict(exclude_unset=True) for item in update_list}
    db_items = owned_transfer_query(db, owner).filter(models.TransferItem.id.in_(updates.keys())).all()
    if len(db_items) < len(updates):
        raise ValidationError("Some TransferItems not found")
    for transfer_item in db_items:
        for k, v in updates[transfer_item.id].items():
            setattr(transfer_item, k, v)
    db.flush()
    updated_jobs, events = _update_jobs(db, [item.job_id for item in db_items])
    return db_items, updated_jobs, events
