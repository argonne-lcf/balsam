from typing import List
from enum import Enum
from dataclasses import dataclass
from datetime import datetime

from fastapi import Depends, APIRouter, status, Query

from balsam import schemas
from balsam.server.models import get_session, crud
from balsam.server.models import Job, Site, BatchJob
from balsam.server.util import Paginator
from balsam.server.pubsub import pubsub
from balsam.server import settings, ValidationError

router = APIRouter()
auth = settings.auth.get_auth_method()


class JobOrdering(str, Enum):
    last_update = "last_update"
    last_update_desc = "-last_update"
    id = "id"
    id_desc = "-id"
    state = "state"
    state_desc = "-state"
    workdir = "workdir"
    workdir_desc = "-workdir"


@dataclass
class JobQuery:
    id: List[int] = Query(None)
    parent_id: List[int] = Query(None)
    app_id: int = Query(None)
    site_id: int = Query(None)
    batch_job_id: int = Query(None)
    last_update_before: datetime = Query(None)
    last_update_after: datetime = Query(None)
    workdir__contains: str = Query(None)
    state__ne: str = Query(None)
    state: List[str] = Query(None)
    tags: List[str] = Query(None)
    parameters: List[str] = Query(None)
    ordering: JobOrdering = Query(None)

    def apply_filters(self, qs):
        if self.id:
            qs = qs.filter(Job.id.in_(self.id))
        if self.parent_id:
            qs = qs.filter(Job.parents.any(Job.id.in_(self.parent_id)))
        if self.app_id:
            qs = qs.filter(Job.app_id == self.app_id)
        if self.site_id:
            qs = qs.filter(Site.id == self.site_id)
        if self.batch_job_id:
            qs = qs.join(BatchJob)
            qs = qs.filter(BatchJob.id == self.batch_job_id)
        if self.last_update_before:
            qs = qs.filter(Job.last_update <= self.last_update_before)
        if self.last_update_after:
            qs = qs.filter(Job.last_update >= self.last_update_after)
        if self.workdir__contains:
            qs = qs.filter(Job.workdir.like(f"%{self.workdir__contains}%"))
        if self.state__ne:
            qs = qs.filter(Job.state != self.state__ne)
        if self.state:
            qs = qs.filter(Job.state.in_(self.state))
        if self.tags:
            tags_dict = dict(t.split(":", 1) for t in self.tags if ":" in t)
            qs = qs.filter(Job.tags.contains(tags_dict))
        if self.parameters:
            params_dict = dict(p.split(":", 1) for p in self.parameters if ":" in p)
            qs = qs.filter(Job.parameters.contains(params_dict))
        if self.ordering:
            desc = self.ordering.startswith("-")
            order_col = getattr(Job, self.ordering.lstrip("-"))
            qs = qs.order_by(order_col.desc() if desc else order_col)
        return qs


@router.get("/", response_model=schemas.PaginatedJobsOut)
def list(
    db=Depends(get_session),
    user=Depends(auth),
    paginator=Depends(Paginator),
    q=Depends(JobQuery),
):
    count, jobs = crud.jobs.fetch(db, owner=user, paginator=paginator, filterset=q)
    return {"count": count, "results": jobs}


@router.get("/{job_id}", response_model=schemas.JobOut)
def read(job_id: int, db=Depends(get_session), user=Depends(auth)):
    count, jobs = crud.jobs.fetch(db, owner=user, job_id=job_id)
    return jobs[0]


@router.post(
    "/", response_model=List[schemas.JobOut], status_code=status.HTTP_201_CREATED
)
def bulk_create(
    jobs: List[schemas.JobCreate], db=Depends(get_session), user=Depends(auth)
):
    new_jobs, new_events, new_transfers = crud.jobs.bulk_create(
        db, owner=user, job_specs=jobs
    )

    result_jobs = [schemas.JobOut.from_orm(job) for job in new_jobs]
    result_events = [schemas.LogEventOut.from_orm(e) for e in new_events]
    result_transfers = [schemas.TransferItemOut.from_orm(t) for t in new_transfers]

    pubsub.publish(user.id, "bulk-create", "job", result_jobs)
    pubsub.publish(user.id, "bulk-create", "event", result_events)
    pubsub.publish(user.id, "bulk-create", "transfer-item", result_transfers)
    return result_jobs


@router.patch("/", response_model=List[schemas.JobOut])
def bulk_update(
    jobs: List[schemas.JobBulkUpdate], db=Depends(get_session), user=Depends(auth)
):
    now = datetime.utcnow()
    patch_dicts = {
        job.id: {**job.dict(exclude_unset=True, exclude={"id"}), "last_update": now}
        for job in jobs
    }
    if len(jobs) > len(patch_dicts):
        raise ValidationError("Duplicate Job ID keys provided")
    updated_jobs, new_events = crud.jobs.bulk_update(
        db, owner=user, patch_dicts=patch_dicts
    )

    result_jobs = [schemas.JobOut.from_orm(job) for job in updated_jobs]
    result_events = [schemas.LogEventOut.from_orm(e) for e in new_events]

    pubsub.publish(user.id, "bulk-update", "job", result_jobs)
    pubsub.publish(user.id, "bulk-create", "event", result_events)
    return result_jobs


@router.put("/", response_model=List[schemas.JobOut])
def query_update(
    update_fields: schemas.JobUpdate,
    db=Depends(get_session),
    user=Depends(auth),
    q=Depends(JobQuery),
):
    data = update_fields.dict(exclude_unset=True)
    data["last_update"] = datetime.utcnow()
    updated_jobs, new_events = crud.jobs.update_query(
        db, owner=user, update_data=data, filterset=q
    )

    result_jobs = [schemas.JobOut.from_orm(job) for job in updated_jobs]
    result_events = [schemas.LogEventOut.from_orm(e) for e in new_events]

    pubsub.publish(user.id, "bulk-update", "job", result_jobs)
    pubsub.publish(user.id, "bulk-create", "event", result_events)
    return result_jobs


@router.put("/{job_id}", response_model=schemas.JobOut)
def update(
    job_id: int, job: schemas.JobUpdate, db=Depends(get_session), user=Depends(auth)
):
    data = job.dict(exclude_unset=True)
    data["last_update"] = datetime.utcnow()
    patch = {job_id: data}
    updated_jobs, new_events = crud.jobs.bulk_update(db, owner=user, patch_dicts=patch)

    result_jobs = [schemas.JobOut.from_orm(job) for job in updated_jobs]
    result_events = [schemas.LogEventOut.from_orm(e) for e in new_events]

    pubsub.publish(user.id, "bulk-update", "job", result_jobs)
    pubsub.publish(user.id, "bulk-create", "event", result_events)
    return result_jobs[0]


@router.delete("/{job_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete(job_id: int, db=Depends(get_session), user=Depends(auth)):
    crud.jobs.delete_query(db, owner=user, job_id=job_id)
    pubsub.publish(user.id, "bulk-delete", "job", {"ids": [job_id]})


@router.delete("/", status_code=status.HTTP_204_NO_CONTENT)
def query_delete(db=Depends(get_session), user=Depends(auth), q=Depends(JobQuery)):
    deleted_ids = crud.jobs.delete_query(db, owner=user, filterset=q)
    pubsub.publish(user.id, "bulk-delete", "job", {"ids": deleted_ids})
