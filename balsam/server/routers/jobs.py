from datetime import datetime
from typing import Any, Dict, List

from fastapi import APIRouter, Depends, status
from sqlalchemy import orm

from balsam import schemas
from balsam.server import ValidationError, settings
from balsam.server.models import Job, crud, get_session
from balsam.server.pubsub import pubsub
from balsam.server.utils import Paginator

from .filters import JobQuery

router = APIRouter()
auth = settings.auth.get_auth_method()


@router.get("/", response_model=schemas.PaginatedJobsOut)
def list(
    db: orm.Session = Depends(get_session),
    user: schemas.UserOut = Depends(auth),
    paginator: Paginator[Job] = Depends(Paginator),
    q: JobQuery = Depends(JobQuery),
) -> Dict[str, Any]:
    """List the user's Jobs."""
    count, jobs = crud.jobs.fetch(db, owner=user, paginator=paginator, filterset=q)
    return {"count": count, "results": jobs}


@router.get("/{job_id}", response_model=schemas.JobOut)
def read(job_id: int, db: orm.Session = Depends(get_session), user: schemas.UserOut = Depends(auth)) -> Job:
    """Get a Job by id."""
    count, jobs = crud.jobs.fetch(db, owner=user, job_id=job_id)
    return jobs[0]


@router.post("/", response_model=List[schemas.JobOut], status_code=status.HTTP_201_CREATED)
def bulk_create(
    jobs: List[schemas.JobCreate], db: orm.Session = Depends(get_session), user: schemas.UserOut = Depends(auth)
) -> List[schemas.JobOut]:
    """Create a list of Jobs."""
    new_jobs, new_events, new_transfers = crud.jobs.bulk_create(db, owner=user, job_specs=jobs)

    result_jobs = [schemas.JobOut.from_orm(job) for job in new_jobs]
    result_events = [schemas.LogEventOut.from_orm(e) for e in new_events]
    result_transfers = [schemas.TransferItemOut.from_orm(t) for t in new_transfers]

    db.commit()
    pubsub.publish(user.id, "bulk-create", "job", result_jobs)
    pubsub.publish(user.id, "bulk-create", "event", result_events)
    pubsub.publish(user.id, "bulk-create", "transfer-item", result_transfers)
    return result_jobs


@router.patch("/", response_model=List[schemas.JobOut])
def bulk_update(
    jobs: List[schemas.JobBulkUpdate],
    db: orm.Session = Depends(get_session),
    user: schemas.UserOut = Depends(auth),
) -> List[schemas.JobOut]:
    """Update a list of Jobs"""
    now = datetime.utcnow()
    patch_dicts = {job.id: {**job.dict(exclude_unset=True, exclude={"id"}), "last_update": now} for job in jobs}
    if len(jobs) > len(patch_dicts):
        raise ValidationError("Duplicate Job ID keys provided")
    updated_jobs, new_events = crud.jobs.bulk_update(db, owner=user, patch_dicts=patch_dicts)

    result_jobs = [schemas.JobOut.from_orm(job) for job in updated_jobs]
    result_events = [schemas.LogEventOut.from_orm(e) for e in new_events]
    db.commit()

    pubsub.publish(user.id, "bulk-update", "job", result_jobs)
    pubsub.publish(user.id, "bulk-create", "event", result_events)
    return result_jobs


@router.put("/", response_model=List[schemas.JobOut])
def query_update(
    update_fields: schemas.JobUpdate,
    db: orm.Session = Depends(get_session),
    user: schemas.UserOut = Depends(auth),
    q: JobQuery = Depends(JobQuery),
) -> List[schemas.JobOut]:
    """Apply the same update to all Jobs selected by the query."""
    data = update_fields.dict(exclude_unset=True)
    data["last_update"] = datetime.utcnow()
    updated_jobs, new_events = crud.jobs.update_query(db, owner=user, update_data=data, filterset=q)

    result_jobs = [schemas.JobOut.from_orm(job) for job in updated_jobs]
    result_events = [schemas.LogEventOut.from_orm(e) for e in new_events]
    db.commit()

    pubsub.publish(user.id, "bulk-update", "job", result_jobs)
    pubsub.publish(user.id, "bulk-create", "event", result_events)
    return result_jobs


@router.put("/{job_id}", response_model=schemas.JobOut)
def update(
    job_id: int, job: schemas.JobUpdate, db: orm.Session = Depends(get_session), user: schemas.UserOut = Depends(auth)
) -> schemas.JobOut:
    """Update a Job by id."""
    data = job.dict(exclude_unset=True)
    data["last_update"] = datetime.utcnow()
    patch = {job_id: data}
    updated_jobs, new_events = crud.jobs.bulk_update(db, owner=user, patch_dicts=patch)

    result_jobs = [schemas.JobOut.from_orm(job) for job in updated_jobs]
    result_events = [schemas.LogEventOut.from_orm(e) for e in new_events]

    db.commit()
    pubsub.publish(user.id, "bulk-update", "job", result_jobs)
    pubsub.publish(user.id, "bulk-create", "event", result_events)
    return result_jobs[0]


@router.delete("/{job_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete(job_id: int, db: orm.Session = Depends(get_session), user: schemas.UserOut = Depends(auth)) -> None:
    """Delete a Job by id."""
    crud.jobs.delete_query(db, owner=user, job_id=job_id)
    db.commit()
    pubsub.publish(user.id, "bulk-delete", "job", {"ids": [job_id]})


@router.delete("/", status_code=status.HTTP_204_NO_CONTENT)
def query_delete(
    db: orm.Session = Depends(get_session), user: schemas.UserOut = Depends(auth), q: JobQuery = Depends(JobQuery)
) -> None:
    """Delete all jobs selected by the query."""
    deleted_ids = crud.jobs.delete_query(db, owner=user, filterset=q)
    db.commit()
    pubsub.publish(user.id, "bulk-delete", "job", {"ids": deleted_ids})
