from typing import Any, Dict

from fastapi import APIRouter, Depends, status
from fastapi.responses import ORJSONResponse
from sqlalchemy import orm

from balsam import schemas
from balsam.server.auth import get_auth_method, get_webuser_session
from balsam.server.models import crud
from balsam.server.pubsub import pubsub

from .filters import SessionQuery

router = APIRouter()
auth = get_auth_method()


@router.get("/", response_model=schemas.PaginatedSessionsOut)
def list(
    db: orm.Session = Depends(get_webuser_session),
    user: schemas.UserOut = Depends(auth),
    q: SessionQuery = Depends(SessionQuery),
) -> Dict[str, Any]:
    """List Job processing Sessions running under the user's Sites."""
    count, sessions = crud.sessions.fetch(db, owner=user, filterset=q)
    return {"count": count, "results": sessions}


@router.post("/", response_model=schemas.SessionOut, status_code=status.HTTP_201_CREATED)
def create(
    session: schemas.SessionCreate,
    db: orm.Session = Depends(get_webuser_session),
    user: schemas.UserOut = Depends(auth),
) -> schemas.SessionOut:
    """Create a new Session for acquiring Jobs to process."""
    created_session = crud.sessions.create(db, owner=user, session=session)
    result = schemas.SessionOut.from_orm(created_session)
    db.commit()
    return result


@router.post("/{session_id}", response_class=ORJSONResponse)
def acquire(
    session_id: int,
    spec: schemas.SessionAcquire,
    db: orm.Session = Depends(get_webuser_session),
    user: schemas.UserOut = Depends(auth),
) -> ORJSONResponse:
    """Acquire Jobs using the given session_id."""
    acquired_jobs = crud.sessions.acquire(db, owner=user, session_id=session_id, spec=spec)
    db.commit()
    return ORJSONResponse(content=acquired_jobs)


@router.put("/{session_id}")
def tick(
    session_id: int, db: orm.Session = Depends(get_webuser_session), user: schemas.UserOut = Depends(auth)
) -> None:
    """Send a heartbeat to extend the given Session by id."""
    crud.sessions.tick(db, owner=user, session_id=session_id)
    db.commit()


@router.delete("/{session_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete(
    session_id: int, db: orm.Session = Depends(get_webuser_session), user: schemas.UserOut = Depends(auth)
) -> None:
    """Delete a Session, freeing associated Jobs."""
    crud.sessions.delete(db, owner=user, session_id=session_id)
    db.commit()
    pubsub.publish(user.id, "delete", "session", {"id": session_id})
