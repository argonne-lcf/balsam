from typing import Any, Dict

from fastapi import APIRouter, Depends
from sqlalchemy import orm

from balsam import schemas
from balsam.server import settings
from balsam.server.models import LogEvent, crud, get_session
from balsam.server.utils import Paginator

from .filters import EventLogQuery

router = APIRouter()
auth = settings.auth.get_auth_method()


@router.get("/", response_model=schemas.PaginatedLogEventOut)
def list(
    db: orm.Session = Depends(get_session),
    user: schemas.UserOut = Depends(auth),
    paginator: Paginator[LogEvent] = Depends(Paginator),
    q: EventLogQuery = Depends(EventLogQuery),
) -> Dict[str, Any]:
    """List events associated with the user's Jobs."""
    count, events = crud.events.fetch(db, owner=user, paginator=paginator, filterset=q)
    return {"count": count, "results": events}
