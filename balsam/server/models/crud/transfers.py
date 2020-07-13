from datetime import datetime
from sqlalchemy import orm
from balsam.server import models, ValidationError


def owned_transfer_query(db, owner):
    return (
        db.query(models.TransferItem)
        .join(models.Job)
        .join(models.App)
        .join(models.Site)
        .filter(models.Site.owner_id == owner.id)
    )


def fetch(db, owner, paginator=None, transfer_id=None, filterset=None):
    qs = owned_transfer_query(db, owner)
    if transfer_id is not None:
        qs = qs.filter(models.TransferItem.id == transfer_id)
        transfer_item = qs.one()
        return 1, [transfer_item]
    if filterset is not None:
        qs = filterset.apply_filters(qs)
    count = qs.group_by(models.TransferItem.id).count()
    transfers = paginator.paginate(qs)
    return count, transfers


def _set_transfer_state(job):
    direction = "in" if job.state == "READY" else "out"
    is_done = all(
        item.state == "done"
        for item in job.transfer_items
        if item.direction == direction
    )
    if is_done:
        job.state = "STAGED_IN" if direction == "in" else "STAGED_OUT"
    return is_done


def _update_jobs(db, job_ids):
    jobs = (
        db.query(models.Job)
        .filter(
            models.Job.id.in_(job_ids), models.Job.state.in_(["READY", "POSTPROCESSED"])
        )
        .options(
            orm.selectinload(models.Job.transfer_items).load_only(
                models.TransferItem.direction, models.TransferItem.state,
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
                job=job, timestamp=now, from_state=old_state, to_state=job.state,
            )
            updates.append(job)
            events.append(event)
    db.flush()
    return updates, events


def update(db, owner, transfer_id, data):
    item = (
        owned_transfer_query(db, owner)
        .filter(models.TransferItem.id == transfer_id)
        .one()
    )
    for k, v in data.dict(exclude_unset=True).items():
        setattr(item, k, v)
    db.flush()
    updated_jobs, events = _update_jobs(db, [item.job_id])
    updated_job = updated_jobs[0] if updated_jobs else None
    event = events[0] if events else None
    return item, updated_job, event


def bulk_update(db, owner, update_list):
    updates = {item.id: item.dict(exclude_unset=True) for item in update_list}
    db_items = (
        owned_transfer_query(db, owner)
        .filter(models.TransferItem.id.in_(updates.keys()))
        .all()
    )
    if len(db_items) < len(updates):
        raise ValidationError("Some TransferItems not found")
    for transfer_item in db_items:
        for k, v in updates[transfer_item.id].items():
            setattr(transfer_item, k, v)
    db.flush()
    updated_jobs, events = _update_jobs(db, [item.job_id for item in db_items])
    return db_items, updated_jobs, events
