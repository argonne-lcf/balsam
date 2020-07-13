from dataclasses import dataclass
from typing import List
from fastapi import Depends, APIRouter, Query

from balsam.server import settings
from balsam.server.util import Paginator
from balsam import schemas
from balsam.server.models import get_session, crud, Job, TransferItem
from balsam.server.pubsub import pubsub

router = APIRouter()
auth = settings.auth.get_auth_method()


@dataclass
class TransferQuery:
    job_id: List[int] = Query(None)
    state: str = Query(None)
    job_state: str = Query(None)
    tags: str = Query(None)

    def apply_filters(self, qs):
        if self.job_id:
            qs = qs.filter(Job.id.in_(self.job_id))
        if self.state:
            qs = qs.filter(TransferItem.state == self.state)
        if self.job_state:
            qs = qs.filter(Job.state == self.job_state)
        if self.tags:
            tags_dict = dict(t.split(":", 1) for t in self.tags if ":" in t)
            qs = qs.filter(Job.tags.contains(tags_dict))
        return qs


@router.get("/", response_model=schemas.PaginatedTransferItemOut)
def list(
    db=Depends(get_session),
    user=Depends(auth),
    paginator=Depends(Paginator),
    q=Depends(TransferQuery),
):
    count, transfers = crud.transfers.fetch(
        db, owner=user, paginator=paginator, filterset=q
    )
    return {"count": count, "results": transfers}


@router.get("/{transfer_id}", response_model=schemas.TransferItemOut)
def read(transfer_id: int, db=Depends(get_session), user=Depends(auth)):
    count, transfers = crud.transfers.fetch(db, owner=user, transfer_id=transfer_id)
    return transfers[0]


@router.put("/{transfer_id}", response_model=schemas.TransferItemOut)
def update(
    transfer_id: int,
    data: schemas.TransferItemUpdate,
    db=Depends(get_session),
    user=Depends(auth),
):
    updated_transfer, updated_job, log_event = crud.transfers.update(
        db, owner=user, transfer_id=transfer_id, data=data
    )
    result = schemas.TransferItemOut.from_orm(updated_transfer)
    pubsub.publish(user.id, "update", "transfer", result)
    if updated_job:
        pubsub.publish(user.id, "update", "job", schemas.JobOut.from_orm(updated_job))
        pubsub.publish(
            user.id, "create", "log-event", schemas.LogEventOut.from_orm(log_event)
        )
    return result


@router.patch("/", response_model=List[schemas.TransferItemOut])
def bulk_update(
    transfers: List[schemas.TransferItemBulkUpdate],
    db=Depends(get_session),
    user=Depends(auth),
):
    updated_transfers, updated_jobs, log_events = crud.transfers.bulk_update(
        db, owner=user, transfers=transfers
    )
    result_transfers = [schemas.TransferItemOut.from_orm(t) for t in updated_transfers]
    result_jobs = [schemas.JobOut.from_orm(j) for j in updated_jobs]
    result_events = [schemas.LogEventOut.from_orm(e) for e in log_events]

    pubsub.publish(user.id, "bulk-update", "transfer", result_transfers)
    if result_jobs or result_events:
        pubsub.publish(user.id, "bulk-update", "job", result_jobs)
        pubsub.publish(user.id, "bulk-create", "event", result_events)
    return result_transfers
