import json
from typing import List

from fastapi import APIRouter, Depends, Query, status

from balsam import schemas
from balsam.server import settings
from balsam.server.models import crud, get_session
from balsam.server.pubsub import pubsub
from balsam.server.util import Paginator

router = APIRouter()
auth = settings.auth.get_auth_method()


@router.get("/", response_model=schemas.PaginatedAppsOut)
def list(
    db=Depends(get_session),
    user=Depends(auth),
    paginator=Depends(Paginator),
    site_id: List[int] = Query(None),
    id: List[int] = Query(None),
    class_path: str = Query(None),
):
    count, apps = crud.apps.fetch(
        db,
        owner=user,
        paginator=paginator,
        filter_site_ids=site_id,
        ids=id,
        class_path=class_path,
    )
    return {"count": count, "results": apps}


@router.get("/{app_id}", response_model=schemas.AppOut)
def read(app_id: int, db=Depends(get_session), user=Depends(auth)):
    _, app = crud.apps.fetch(db, owner=user, app_id=app_id)
    return app


@router.post("/", response_model=schemas.AppOut, status_code=status.HTTP_201_CREATED)
def create(app: schemas.AppCreate, db=Depends(get_session), user=Depends(auth)):
    """
    Register a new Balsam App
    """
    new_app = crud.apps.create(db, owner=user, app=app)
    result = schemas.AppOut.from_orm(new_app)
    db.commit()
    pubsub.publish(user.id, "create", "app", result)
    return result


@router.put("/{app_id}", response_model=schemas.AppOut)
def update(app_id: int, app: schemas.AppUpdate, db=Depends(get_session), user=Depends(auth)):
    data = json.loads(app.json(exclude_unset=True))
    updated_app = crud.apps.update(db, owner=user, app_id=app_id, update_data=data)
    data["id"] = app_id
    db.commit()
    pubsub.publish(user.id, "update", "app", data)
    return updated_app


@router.delete("/{app_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete(app_id: int, db=Depends(get_session), user=Depends(auth)):
    crud.apps.delete(db, owner=user, app_id=app_id)
    db.commit()
    pubsub.publish(user.id, "delete", "app", {"id": app_id})
