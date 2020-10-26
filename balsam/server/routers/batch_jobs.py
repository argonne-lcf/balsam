from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import List
from fastapi import Depends, APIRouter, status, Query

from balsam.server import settings
from balsam.server.util import Paginator
from balsam import schemas
from balsam.server.models import get_session, crud, BatchJob
from balsam.server.pubsub import pubsub

router = APIRouter()
auth = settings.auth.get_auth_method()


class BatchJobOrdering(str, Enum):
    start_time = "start_time"
    start_time_desc = "-start_time"


@dataclass
class BatchJobQuery:
    site_id: List[int] = Query(None)
    state: List[str] = Query(None)
    ordering: BatchJobOrdering = Query(None)
    start_time_before: datetime = Query(None)
    start_time_after: datetime = Query(None)
    end_time_before: datetime = Query(None)
    end_time_after: datetime = Query(None)
    filter_tags: List[str] = Query(None)

    def apply_filters(self, qs):
        if self.site_id:
            qs = qs.filter(BatchJob.site_id.in_(self.site_id))
        if self.state:
            qs = qs.filter(BatchJob.state.in_(self.state))

        if self.start_time_before:
            qs = qs.filter(BatchJob.start_time <= self.start_time_before)
        if self.start_time_after:
            qs = qs.filter(BatchJob.start_time >= self.start_time_after)

        if self.end_time_before:
            qs = qs.filter(BatchJob.end_time <= self.end_time_before)
        if self.end_time_after:
            qs = qs.filter(BatchJob.end_time >= self.end_time_after)

        if self.filter_tags:
            tags_dict = dict(t.split(":", 1) for t in self.filter_tags if ":" in t)
            qs = qs.filter(BatchJob.filter_tags.contains(tags_dict))
        if self.ordering:
            desc = self.ordering.startswith("-")
            order_col = getattr(BatchJob, self.ordering.lstrip("-"))
            qs = qs.order_by(order_col.desc() if desc else order_col)
        return qs


@router.get("/", response_model=schemas.PaginatedBatchJobOut)
def list(
    db=Depends(get_session),
    user=Depends(auth),
    paginator=Depends(Paginator),
    q=Depends(BatchJobQuery),
):
    count, batch_jobs = crud.batch_jobs.fetch(
        db, owner=user, paginator=paginator, filterset=q
    )
    return {"count": count, "results": batch_jobs}


@router.get("/{batch_job_id}", response_model=schemas.BatchJobOut)
def read(batch_job_id: int, db=Depends(get_session), user=Depends(auth)):
    count, batch_jobs = crud.batch_jobs.fetch(db, owner=user, batch_job_id=batch_job_id)
    return batch_jobs[0]


@router.post(
    "/", response_model=schemas.BatchJobOut, status_code=status.HTTP_201_CREATED
)
def create(
    batch_job: schemas.BatchJobCreate, db=Depends(get_session), user=Depends(auth)
):
    new_batch_job = crud.batch_jobs.create(db, owner=user, batch_job=batch_job)
    result = schemas.BatchJobOut.from_orm(new_batch_job)
    db.commit()
    pubsub.publish(user.id, "create", "batch_job", result)
    return result


@router.put("/{batch_job_id}", response_model=schemas.BatchJobOut)
def update(
    batch_job_id: int,
    batch_job: schemas.BatchJobUpdate,
    db=Depends(get_session),
    user=Depends(auth),
):
    updated_batch_job = crud.batch_jobs.update(
        db, owner=user, batch_job_id=batch_job_id, batch_job=batch_job
    )
    result = schemas.BatchJobOut.from_orm(updated_batch_job)
    db.commit()
    pubsub.publish(user.id, "update", "batch_job", result)
    return result


@router.patch("/", response_model=List[schemas.BatchJobOut])
def bulk_update(
    batch_jobs: List[schemas.BatchJobBulkUpdate],
    db=Depends(get_session),
    user=Depends(auth),
):
    updated_batch_jobs = crud.batch_jobs.bulk_update(
        db, owner=user, batch_jobs=batch_jobs
    )
    result = [schemas.BatchJobOut.from_orm(j) for j in updated_batch_jobs]
    db.commit()
    pubsub.publish(user.id, "bulk-update", "batch_job", result)
    return result


@router.delete("/{batch_job_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete(batch_job_id: int, db=Depends(get_session), user=Depends(auth)):
    crud.batch_jobs.delete(db, owner=user, batch_job_id=batch_job_id)
    db.commit()
    pubsub.publish(user.id, "delete", "batch_job", {"id": batch_job_id})
