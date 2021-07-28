from typing import Any, Dict, List, Optional, Tuple, Union, cast

from sqlalchemy.orm import Query, Session

from balsam import schemas
from balsam.server import ValidationError, models
from balsam.server.routers.filters import BatchJobQuery
from balsam.server.utils import Paginator


def fetch(
    db: Session,
    owner: schemas.UserOut,
    paginator: Optional[Paginator[models.BatchJob]] = None,
    batch_job_id: Optional[int] = None,
    filterset: Optional[BatchJobQuery] = None,
) -> "Tuple[int, Union[List[models.BatchJob], Query[models.BatchJob]]]":
    qs: "Query[models.BatchJob]" = db.query(models.BatchJob).join(models.Site).filter(models.Site.owner_id == owner.id)  # type: ignore
    if filterset is not None:
        qs = filterset.apply_filters(qs)
    if batch_job_id is not None:
        qs = qs.filter(models.BatchJob.id == batch_job_id)
        return 1, [qs.one()]
    count = qs.group_by(models.BatchJob.id).count()
    assert paginator is not None
    batch_jobs = paginator.paginate(qs)
    return count, batch_jobs


def create(db: Session, owner: schemas.UserOut, batch_job: schemas.BatchJobCreate) -> models.BatchJob:
    site_id = (
        db.query(models.Site.id)
        .filter(models.Site.owner_id == owner.id, models.Site.id == batch_job.site_id)
        .scalar()  # type: ignore
    )
    if site_id is None:
        raise ValidationError(f"No site with ID {batch_job.site_id}")
    created_batch_job = models.BatchJob(state=schemas.BatchJobState.pending_submission, **batch_job.dict())
    db.add(created_batch_job)
    db.flush()
    return created_batch_job


def _update_fields(in_db: models.BatchJob, update_dict: Dict[str, Any]) -> None:
    state_update = update_dict.pop("state", in_db.state)
    for k, v in update_dict.items():
        setattr(in_db, k, v)
    if in_db.state == schemas.BatchJobState.pending_deletion:
        if state_update != schemas.BatchJobState.finished:
            return
    in_db.state = state_update


def update(
    db: Session, owner: schemas.UserOut, batch_job_id: int, batch_job: schemas.BatchJobUpdate
) -> models.BatchJob:
    in_db = cast(
        models.BatchJob,
        db.query(models.BatchJob)
        .join(models.Site)  # type: ignore
        .filter(models.Site.owner_id == owner.id)
        .filter(models.BatchJob.id == batch_job_id)
        .with_for_update(of=models.BatchJob, nowait=False, skip_locked=False)
        .one(),
    )
    _update_fields(in_db, batch_job.dict(exclude_unset=True))
    db.flush()
    return in_db


def bulk_update(
    db: Session, owner: schemas.UserOut, batch_jobs: List[schemas.BatchJobBulkUpdate]
) -> List[models.BatchJob]:
    update_map = {j.id: j.dict(exclude_unset=True) for j in batch_jobs}
    ids = set(update_map.keys())
    qs: "Query[models.BatchJob]" = (
        db.query(models.BatchJob)
        .join(models.Site)  # type: ignore
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


def delete(db: Session, owner: schemas.UserOut, batch_job_id: int) -> None:
    qs = (
        db.query(models.BatchJob)
        .join(models.Site)  # type: ignore
        .filter(models.Site.owner_id == owner.id)
        .filter(models.BatchJob.id == batch_job_id)
    )
    bjob = qs.one()
    db.query(models.BatchJob).filter(models.BatchJob.id == bjob.id).delete()
    db.flush()
