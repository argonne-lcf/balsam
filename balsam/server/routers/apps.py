import json
from typing import Any, Dict

from fastapi import APIRouter, Depends, status
from sqlalchemy import orm

from balsam import schemas
from balsam.server import models, settings
from balsam.server.models import crud, get_session
from balsam.server.pubsub import pubsub
from balsam.server.utils import Paginator

from .filters import AppQuery

router = APIRouter()
auth = settings.auth.get_auth_method()


@router.get("/", response_model=schemas.PaginatedAppsOut)
def list(
    db: orm.Session = Depends(get_session),
    user: schemas.UserOut = Depends(auth),
    paginator: Paginator[models.App] = Depends(Paginator),
    q: AppQuery = Depends(AppQuery),
) -> Dict[str, Any]:
    """List Apps registered by the user's Balsam Sites."""
    count, apps = crud.apps.fetch(
        db,
        owner=user,
        paginator=paginator,
        filterset=q,
    )
    return {"count": count, "results": apps}


@router.get("/{app_id}", response_model=schemas.AppOut)
def read(
    app_id: int, db: orm.Session = Depends(get_session), user: schemas.UserOut = Depends(auth)
) -> "orm.Query[models.App]":
    _, app = crud.apps.fetch(db, owner=user, app_id=app_id)
    """Get the specified App."""
    return app


@router.post("/", response_model=schemas.AppOut, status_code=status.HTTP_201_CREATED)
def create(
    app: schemas.AppCreate, db: orm.Session = Depends(get_session), user: schemas.UserOut = Depends(auth)
) -> schemas.AppOut:
    """
    Register a new Balsam App.
    """
    new_app = crud.apps.create(db, owner=user, app=app)
    result = schemas.AppOut.from_orm(new_app)
    db.commit()
    pubsub.publish(user.id, "create", "app", result)
    return result


@router.put("/{app_id}", response_model=schemas.AppOut)
def update(
    app_id: int, app: schemas.AppUpdate, db: orm.Session = Depends(get_session), user: schemas.UserOut = Depends(auth)
) -> models.App:
    """Update an App."""
    data = json.loads(app.json(exclude_unset=True))
    updated_app = crud.apps.update(db, owner=user, app_id=app_id, update_data=data)
    data["id"] = app_id
    db.commit()
    pubsub.publish(user.id, "update", "app", data)
    return updated_app


@router.delete("/{app_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete(app_id: int, db: orm.Session = Depends(get_session), user: schemas.UserOut = Depends(auth)) -> None:
    """Delete an App and all associated Jobs are deleted."""
    crud.apps.delete(db, owner=user, app_id=app_id)
    db.commit()
    pubsub.publish(user.id, "delete", "app", {"id": app_id})
