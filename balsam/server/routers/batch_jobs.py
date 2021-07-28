from typing import Any, Dict, List

from fastapi import APIRouter, Depends, status
from sqlalchemy import orm

from balsam import schemas
from balsam.server import settings
from balsam.server.models import BatchJob, crud, get_session
from balsam.server.pubsub import pubsub
from balsam.server.utils import Paginator

from .filters import BatchJobQuery

router = APIRouter()
auth = settings.auth.get_auth_method()


@router.get("/", response_model=schemas.PaginatedBatchJobOut)
def list(
    db: orm.Session = Depends(get_session),
    user: schemas.UserOut = Depends(auth),
    paginator: Paginator[BatchJob] = Depends(Paginator),
    q: BatchJobQuery = Depends(BatchJobQuery),
) -> Dict[str, Any]:
    """List the BatchJobs submitted at the user's Balsam Sites."""
    count, batch_jobs = crud.batch_jobs.fetch(db, owner=user, paginator=paginator, filterset=q)
    return {"count": count, "results": batch_jobs}


@router.get("/{batch_job_id}", response_model=schemas.BatchJobOut)
def read(
    batch_job_id: int, db: orm.Session = Depends(get_session), user: schemas.UserOut = Depends(auth)
) -> BatchJob:
    """Get a BatchJob by id."""
    count, batch_jobs = crud.batch_jobs.fetch(db, owner=user, batch_job_id=batch_job_id)
    return batch_jobs[0]


@router.post("/", response_model=schemas.BatchJobOut, status_code=status.HTTP_201_CREATED)
def create(
    batch_job: schemas.BatchJobCreate, db: orm.Session = Depends(get_session), user: schemas.UserOut = Depends(auth)
) -> schemas.BatchJobOut:
    """Submit a new BatchJob to a Balsam Site's queue."""
    new_batch_job = crud.batch_jobs.create(db, owner=user, batch_job=batch_job)
    result = schemas.BatchJobOut.from_orm(new_batch_job)
    db.commit()
    pubsub.publish(user.id, "create", "batch_job", result)
    return result


@router.put("/{batch_job_id}", response_model=schemas.BatchJobOut)
def update(
    batch_job_id: int,
    batch_job: schemas.BatchJobUpdate,
    db: orm.Session = Depends(get_session),
    user: schemas.UserOut = Depends(auth),
) -> schemas.BatchJobOut:
    """Update a BatchJob by id."""
    updated_batch_job = crud.batch_jobs.update(db, owner=user, batch_job_id=batch_job_id, batch_job=batch_job)
    result = schemas.BatchJobOut.from_orm(updated_batch_job)
    db.commit()
    pubsub.publish(user.id, "update", "batch_job", result)
    return result


@router.patch("/", response_model=List[schemas.BatchJobOut])
def bulk_update(
    batch_jobs: List[schemas.BatchJobBulkUpdate],
    db: orm.Session = Depends(get_session),
    user: schemas.UserOut = Depends(auth),
) -> List[schemas.BatchJobOut]:
    """Update a list of BatchJobs."""
    updated_batch_jobs = crud.batch_jobs.bulk_update(db, owner=user, batch_jobs=batch_jobs)
    result = [schemas.BatchJobOut.from_orm(j) for j in updated_batch_jobs]
    db.commit()
    pubsub.publish(user.id, "bulk-update", "batch_job", result)
    return result


@router.delete("/{batch_job_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete(batch_job_id: int, db: orm.Session = Depends(get_session), user: schemas.UserOut = Depends(auth)) -> None:
    """Delete a BatchJob by id."""
    crud.batch_jobs.delete(db, owner=user, batch_job_id=batch_job_id)
    db.commit()
    pubsub.publish(user.id, "delete", "batch_job", {"id": batch_job_id})
