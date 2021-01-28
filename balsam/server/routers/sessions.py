from typing import List

from fastapi import APIRouter, Depends, status

from balsam import schemas
from balsam.server import settings
from balsam.server.models import crud, get_session
from balsam.server.pubsub import pubsub

router = APIRouter()
auth = settings.auth.get_auth_method()


@router.get("/", response_model=schemas.PaginatedSessionsOut)
def list(db=Depends(get_session), user=Depends(auth)):
    count, sessions = crud.sessions.fetch(db, owner=user)
    return {"count": count, "results": sessions}


@router.post("/", response_model=schemas.SessionOut, status_code=status.HTTP_201_CREATED)
def create(session: schemas.SessionCreate, db=Depends(get_session), user=Depends(auth)):
    created_session, expired_jobs, expiry_events = crud.sessions.create(db, owner=user, session=session)
    result = schemas.SessionOut.from_orm(created_session)

    result_jobs = [schemas.JobOut.from_orm(job) for job in expired_jobs]
    result_events = [schemas.LogEventOut.from_orm(e) for e in expiry_events]
    db.commit()

    pubsub.publish(user.id, "create", "session", result)
    pubsub.publish(user.id, "bulk-update", "job", result_jobs)
    pubsub.publish(user.id, "bulk-create", "event", result_events)
    return result


@router.post("/{session_id}", response_model=List[schemas.JobOut])
def acquire(
    session_id: int,
    spec: schemas.SessionAcquire,
    db=Depends(get_session),
    user=Depends(auth),
):
    acquired_jobs, expired_jobs, expiry_events = crud.sessions.acquire(
        db, owner=user, session_id=session_id, spec=spec
    )
    result_jobs = [schemas.JobOut.from_orm(job) for job in expired_jobs]
    result_events = [schemas.LogEventOut.from_orm(e) for e in expiry_events]
    db.commit()
    pubsub.publish(user.id, "bulk-update", "job", result_jobs)
    pubsub.publish(user.id, "bulk-create", "event", result_events)
    return acquired_jobs


@router.put("/{session_id}")
def tick(session_id: int, db=Depends(get_session), user=Depends(auth)):
    ts, expired_jobs, events = crud.sessions.tick(db, owner=user, session_id=session_id)
    result = {"id": session_id, "heartbeat": ts}
    pubsub.publish(user.id, "update", "session", result)
    result_jobs = [schemas.JobOut.from_orm(job) for job in expired_jobs]
    result_events = [schemas.LogEventOut.from_orm(e) for e in events]
    db.commit()
    pubsub.publish(user.id, "bulk-update", "job", result_jobs)
    pubsub.publish(user.id, "bulk-create", "event", result_events)


@router.delete("/{session_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete(session_id: int, db=Depends(get_session), user=Depends(auth)):
    crud.sessions.delete(db, owner=user, session_id=session_id)
    db.commit()
    pubsub.publish(user.id, "delete", "session", {"id": session_id})
