from datetime import datetime
from typing import List

import orjson
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import ORJSONResponse
from sqlalchemy import orm
from starlette.responses import Response

from balsam import schemas
from balsam.schemas import MAX_ITEMS_PER_BULK_OP
from balsam.server import ValidationError
from balsam.server.auth import get_auth_method, get_webuser_session
from balsam.server.models import Job, crud
from balsam.server.pubsub import pubsub
from balsam.server.utils import Paginator

from .filters import JobQuery

router = APIRouter()
auth = get_auth_method()


@router.get("/", response_class=Response)
def list(
    db: orm.Session = Depends(get_webuser_session),
    user: schemas.UserOut = Depends(auth),
    paginator: Paginator[Job] = Depends(Paginator),
    q: JobQuery = Depends(JobQuery),
) -> Response:
    """List the user's Jobs."""
    count, jobs = crud.jobs.fetch(db, owner=user, paginator=paginator, filterset=q)
    return Response(content=orjson.dumps({"count": count, "results": jobs}), media_type="application/json")


@router.get("/{job_id}", response_class=ORJSONResponse)
def read(
    job_id: int, db: orm.Session = Depends(get_webuser_session), user: schemas.UserOut = Depends(auth)
) -> ORJSONResponse:
    """Get a Job by id."""
    count, jobs = crud.jobs.fetch(db, owner=user, job_id=job_id)
    return ORJSONResponse(content=jobs[0])


@router.post("/", response_class=ORJSONResponse, status_code=status.HTTP_201_CREATED)
def bulk_create(
    jobs: List[schemas.ServerJobCreate],
    db: orm.Session = Depends(get_webuser_session),
    user: schemas.UserOut = Depends(auth),
) -> ORJSONResponse:
    """Create a list of Jobs."""
    if len(jobs) > MAX_ITEMS_PER_BULK_OP:
        raise HTTPException(
            status_code=400, detail=f"Cannot bulk-create more than {MAX_ITEMS_PER_BULK_OP} in a single API call."
        )
    new_jobs = crud.jobs.bulk_create(db, owner=user, job_specs=jobs)
    db.commit()
    # TODO: Pubsub.publish using jsonable_encoder: killing performance for many jobs
    # If re-integrating pubsub, need to root out fastapi.jsonable_encoder.
    return ORJSONResponse(content=new_jobs, status_code=status.HTTP_201_CREATED)


@router.patch("/")
def bulk_update(
    jobs: List[schemas.JobBulkUpdate],
    db: orm.Session = Depends(get_webuser_session),
    user: schemas.UserOut = Depends(auth),
) -> int:
    """Update a list of Jobs"""
    if len(jobs) > MAX_ITEMS_PER_BULK_OP:
        raise HTTPException(
            status_code=400, detail=f"Cannot bulk-update more than {MAX_ITEMS_PER_BULK_OP} in a single API call."
        )
    now = datetime.utcnow()
    patch_dicts = {job.id: {**job.dict(exclude_unset=True, exclude={"id"}), "last_update": now} for job in jobs}
    if len(jobs) > len(patch_dicts):
        raise ValidationError("Duplicate Job ID keys provided")
    num_updated = crud.jobs.bulk_update(db, owner=user, patch_dicts=patch_dicts)
    db.commit()
    return num_updated


@router.put("/")
def query_update(
    update_fields: schemas.JobUpdate,
    db: orm.Session = Depends(get_webuser_session),
    user: schemas.UserOut = Depends(auth),
    q: JobQuery = Depends(JobQuery),
) -> int:
    """Apply the same update to all Jobs selected by the query."""
    data = update_fields.dict(exclude_unset=True)
    data["last_update"] = datetime.utcnow()
    num_updated = crud.jobs.update_query(db, owner=user, update_data=data, filterset=q)
    db.commit()
    return num_updated


@router.put("/{job_id}")
def update(
    job_id: int,
    job: schemas.JobUpdate,
    db: orm.Session = Depends(get_webuser_session),
    user: schemas.UserOut = Depends(auth),
) -> int:
    """Update a Job by id."""
    data = job.dict(exclude_unset=True)
    data["last_update"] = datetime.utcnow()
    patch = {job_id: data}
    num_updated = crud.jobs.bulk_update(db, owner=user, patch_dicts=patch)
    db.commit()
    return num_updated


@router.delete("/{job_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete(
    job_id: int, db: orm.Session = Depends(get_webuser_session), user: schemas.UserOut = Depends(auth)
) -> None:
    """Delete a Job by id."""
    crud.jobs.delete_query(db, owner=user, job_id=job_id)
    db.commit()
    pubsub.publish(user.id, "bulk-delete", "job", {"ids": [job_id]})


@router.delete("/", status_code=status.HTTP_204_NO_CONTENT)
def query_delete(
    db: orm.Session = Depends(get_webuser_session),
    user: schemas.UserOut = Depends(auth),
    q: JobQuery = Depends(JobQuery),
) -> int:
    """Delete all jobs selected by the query."""
    num_deleted = crud.jobs.delete_query(db, owner=user, filterset=q)
    db.commit()
    return num_deleted
