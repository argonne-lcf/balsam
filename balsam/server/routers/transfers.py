from typing import Any, Dict, List

from fastapi import APIRouter, Depends
from sqlalchemy import orm

from balsam import schemas
from balsam.server.auth import get_auth_method, get_webuser_session
from balsam.server.models import TransferItem, crud
from balsam.server.utils import Paginator

from .filters import TransferItemQuery

router = APIRouter()
auth = get_auth_method()


@router.get("/", response_model=schemas.PaginatedTransferItemOut)
def list(
    db: orm.Session = Depends(get_webuser_session),
    user: schemas.UserOut = Depends(auth),
    paginator: Paginator[TransferItem] = Depends(Paginator),
    q: TransferItemQuery = Depends(TransferItemQuery),
) -> Dict[str, Any]:
    """List data transfers associated with the user's Jobs."""
    count, transfers = crud.transfers.fetch(db, owner=user, paginator=paginator, filterset=q)
    return {"count": count, "results": transfers}


@router.get("/{transfer_id}", response_model=schemas.TransferItemOut)
def read(
    transfer_id: int, db: orm.Session = Depends(get_webuser_session), user: schemas.UserOut = Depends(auth)
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
    db: orm.Session = Depends(get_webuser_session),
    user: schemas.UserOut = Depends(auth),
) -> schemas.TransferItemOut:
    """Update a transfer item by id."""
    updated_transfer = crud.transfers.update(db, owner=user, transfer_id=transfer_id, data=data)
    result = schemas.TransferItemOut.from_orm(updated_transfer)
    db.commit()
    return result


@router.patch("/", response_model=List[schemas.TransferItemOut])
def bulk_update(
    transfers: List[schemas.TransferItemBulkUpdate],
    db: orm.Session = Depends(get_webuser_session),
    user: schemas.UserOut = Depends(auth),
) -> List[schemas.TransferItemOut]:
    """Update a list of transfer items."""
    updated_transfers = crud.transfers.bulk_update(db, owner=user, update_list=transfers)
    result_transfers = [schemas.TransferItemOut.from_orm(t) for t in updated_transfers]
    db.commit()
    return result_transfers
