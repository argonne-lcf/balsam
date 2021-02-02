from dataclasses import dataclass
from typing import List, Set, Tuple, cast

from fastapi import APIRouter, Depends, Query

from balsam import schemas
from balsam.server import settings
from balsam.server.models import Job, Site, TransferItem, crud, get_session
from balsam.server.pubsub import pubsub
from balsam.server.util import FilterSet, Paginator

router = APIRouter()
auth = settings.auth.get_auth_method()


@dataclass
class TransferQuery(FilterSet):
    id: List[int] = Query(None)
    site_id: int = Query(None)
    job_id: List[int] = Query(None)
    state: Set[schemas.TransferItemState] = Query(None)
    direction: schemas.TransferDirection = Query(None)
    job_state: str = Query(None)
    tags: List[str] = Query(None)

    def apply_filters(self, qs):
        if self.id:
            qs = qs.filter(TransferItem.id.in_(self.id))
        if self.site_id:
            qs = qs.filter(Site.id == self.site_id)
        if self.job_id:
            qs = qs.filter(Job.id.in_(self.job_id))
        if self.state:
            qs = qs.filter(TransferItem.state.in_(self.state))
        if self.direction:
            qs = qs.filter(TransferItem.direction == self.direction)
        if self.job_state:
            qs = qs.filter(Job.state == self.job_state)
        if self.tags:
            tags_dict = dict(cast(Tuple[str, str], t.split(":", 1)) for t in self.tags if ":" in t)
            qs = qs.filter(Job.tags.contains(tags_dict))
        return qs


@router.get("/", response_model=schemas.PaginatedTransferItemOut)
def list(
    db=Depends(get_session),
    user=Depends(auth),
    paginator=Depends(Paginator),
    q=Depends(TransferQuery),
):
    count, transfers = crud.transfers.fetch(db, owner=user, paginator=paginator, filterset=q)
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
    db.commit()
    pubsub.publish(user.id, "update", "transfer", result)
    if updated_job:
        pubsub.publish(user.id, "update", "job", schemas.JobOut.from_orm(updated_job))
        pubsub.publish(user.id, "create", "log-event", schemas.LogEventOut.from_orm(log_event))
    return result


@router.patch("/", response_model=List[schemas.TransferItemOut])
def bulk_update(
    transfers: List[schemas.TransferItemBulkUpdate],
    db=Depends(get_session),
    user=Depends(auth),
):
    updated_transfers, updated_jobs, log_events = crud.transfers.bulk_update(db, owner=user, update_list=transfers)
    result_transfers = [schemas.TransferItemOut.from_orm(t) for t in updated_transfers]
    result_jobs = [schemas.JobOut.from_orm(j) for j in updated_jobs]
    result_events = [schemas.LogEventOut.from_orm(e) for e in log_events]
    db.commit()

    pubsub.publish(user.id, "bulk-update", "transfer", result_transfers)
    if result_jobs or result_events:
        pubsub.publish(user.id, "bulk-update", "job", result_jobs)
        pubsub.publish(user.id, "bulk-create", "event", result_events)
    return result_transfers
