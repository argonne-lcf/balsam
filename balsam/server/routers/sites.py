from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy import orm

from balsam import schemas
from balsam.server import settings
from balsam.server.models import Site, crud, get_session
from balsam.server.pubsub import pubsub
from balsam.server.util import Paginator

router = APIRouter()
auth = settings.auth.get_auth_method()


@router.get("/", response_model=schemas.PaginatedSitesOut)
def list(
    hostname: Optional[str] = None,
    path: Optional[str] = None,
    id: List[int] = Query(None),
    db: orm.Session = Depends(get_session),
    user: schemas.UserOut = Depends(auth),
    paginator: Paginator[Site] = Depends(Paginator),
) -> Dict[str, Any]:
    count, sites = crud.sites.fetch(
        db,
        owner=user,
        paginator=paginator,
        host_contains=hostname,
        path_contains=path,
        ids=id,
    )
    return {"count": count, "results": sites}


@router.get("/{site_id}", response_model=schemas.SiteOut)
def read(site_id: int, db: orm.Session = Depends(get_session), user: schemas.UserOut = Depends(auth)) -> Site:
    _, site = crud.sites.fetch(db, owner=user, site_id=site_id)
    assert isinstance(site, Site)
    return site


@router.post("/", response_model=schemas.SiteOut, status_code=status.HTTP_201_CREATED)
def create(
    site: schemas.SiteCreate,
    db: orm.Session = Depends(get_session),
    user: schemas.UserOut = Depends(auth),
) -> schemas.SiteOut:
    new_site = crud.sites.create(db, owner=user, site=site)
    result = schemas.SiteOut.from_orm(new_site)
    db.commit()
    pubsub.publish(user.id, "create", "site", result)
    return result


@router.put("/{site_id}", response_model=schemas.SiteOut)
def update(
    site_id: int,
    site: schemas.SiteUpdate,
    db: orm.Session = Depends(get_session),
    user: schemas.UserOut = Depends(auth),
) -> Site:
    data = site.dict(exclude_unset=True)
    data["last_refresh"] = datetime.utcnow()
    updated_site = crud.sites.update(db, owner=user, site_id=site_id, update_data=data)
    data["id"] = site_id
    db.commit()
    pubsub.publish(user.id, "update", "site", data)
    return updated_site


@router.delete("/{site_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete(site_id: int, db: orm.Session = Depends(get_session), user: schemas.UserOut = Depends(auth)) -> None:
    crud.sites.delete(db, owner=user, site_id=site_id)
    db.commit()
    pubsub.publish(user.id, "delete", "site", {"id": site_id})
