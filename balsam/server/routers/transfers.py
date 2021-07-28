from typing import Any, Dict, List

from fastapi import APIRouter, Depends
from sqlalchemy import orm

from balsam import schemas
from balsam.server import settings
from balsam.server.models import TransferItem, crud, get_session
from balsam.server.pubsub import pubsub
from balsam.server.utils import Paginator

from .filters import TransferItemQuery

router = APIRouter()
auth = settings.auth.get_auth_method()


@router.get("/", response_model=schemas.PaginatedTransferItemOut)
def list(
    db: orm.Session = Depends(get_session),
    user: schemas.UserOut = Depends(auth),
    paginator: Paginator[TransferItem] = Depends(Paginator),
    q: TransferItemQuery = Depends(TransferItemQuery),
) -> Dict[str, Any]:
    """List data transfers associated with the user's Jobs."""
    count, transfers = crud.transfers.fetch(db, owner=user, paginator=paginator, filterset=q)
    return {"count": count, "results": transfers}


@router.get("/{transfer_id}", response_model=schemas.TransferItemOut)
def read(
    transfer_id: int, db: orm.Session = Depends(get_session), user: schemas.UserOut = Depends(auth)
) -> TransferItem:
    """Fetch a data transfer item by id."""
    count, transfers = crud.transfers.fetch(db, owner=user, transfer_id=transfer_id)
    assert isinstance(transfers, List)
    item: TransferItem = transfers[0]
    return item


@router.put("/{transfer_id}", response_model=schemas.TransferItemOut)
def update(
    transfer_id: int,
    data: schemas.TransferItemUpdate,
    db: orm.Session = Depends(get_session),
    user: schemas.UserOut = Depends(auth),
) -> schemas.TransferItemOut:
    """Update a transfer item by id."""
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
    db: orm.Session = Depends(get_session),
    user: schemas.UserOut = Depends(auth),
) -> List[schemas.TransferItemOut]:
    """Update a list of transfer items."""
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
