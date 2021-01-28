from balsam import schemas
from balsam.server import ValidationError, models


def fetch(db, owner, paginator=None, batch_job_id=None, filterset=None):
    qs = db.query(models.BatchJob).join(models.Site).filter(models.Site.owner_id == owner.id)
    if filterset is not None:
        qs = filterset.apply_filters(qs)
    if batch_job_id is not None:
        qs = qs.filter(models.BatchJob.id == batch_job_id)
        return 1, [qs.one()]
    count = qs.group_by(models.BatchJob.id).count()
    batch_jobs = paginator.paginate(qs)
    return count, batch_jobs


def create(db, owner, batch_job):
    site_id = (
        db.query(models.Site.id)
        .filter(models.Site.owner_id == owner.id, models.Site.id == batch_job.site_id)
        .scalar()
    )
    if site_id is None:
        raise ValidationError(f"No site with ID {batch_job.site_id}")
    created_batch_job = models.BatchJob(state=schemas.BatchJobState.pending_submission, **batch_job.dict())
    db.add(created_batch_job)
    db.flush()
    return created_batch_job


def _update_fields(in_db, update_dict):
    state_update = update_dict.pop("state", in_db.state)
    for k, v in update_dict.items():
        setattr(in_db, k, v)
    if in_db.state == schemas.BatchJobState.pending_deletion:
        if state_update != schemas.BatchJobState.finished:
            return
    in_db.state = state_update


def update(db, owner, batch_job_id, batch_job):
    in_db = (
        db.query(models.BatchJob)
        .join(models.Site)
        .filter(models.Site.owner_id == owner.id)
        .filter(models.BatchJob.id == batch_job_id)
        .with_for_update(of=models.BatchJob, nowait=False, skip_locked=False)
        .one()
    )
    _update_fields(in_db, batch_job.dict(exclude_unset=True))
    db.flush()
    return in_db


def bulk_update(db, owner, batch_jobs):
    update_map = {j.id: j.dict(exclude_unset=True) for j in batch_jobs}
    ids = set(update_map.keys())
    qs = (
        db.query(models.BatchJob)
        .join(models.Site)
        .filter(models.Site.owner_id == owner.id)
        .filter(models.BatchJob.id.in_(ids))
        .order_by(models.BatchJob.id)
        .with_for_update(of=models.BatchJob)
    )
    db_jobs = {j.id: j for j in qs.all()}
    if len(db_jobs) < len(update_map):
        raise ValidationError("Could not find some Batch Job IDs")

    for id, update_dict in update_map.items():
        db_job = db_jobs[id]
        _update_fields(db_job, update_dict)

    db.flush()
    return list(db_jobs.values())


def delete(db, owner, batch_job_id):
    qs = (
        db.query(models.BatchJob)
        .join(models.Site)
        .filter(models.Site.owner_id == owner.id)
        .filter(models.BatchJob.id == batch_job_id)
    )
    bjob = qs.one()
    db.query(models.BatchJob).filter(models.BatchJob.id == bjob.id).delete()
    db.flush()
