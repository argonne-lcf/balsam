from datetime import datetime
from typing import Any, Dict, Optional, Tuple, Union

from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Query, Session

from balsam import schemas
from balsam.server import ValidationError, models
from balsam.server.routers.filters import SiteQuery
from balsam.server.utils import Paginator


def fetch(
    db: Session,
    owner: schemas.UserOut,
    paginator: Optional[Paginator[models.Site]] = None,
    site_id: Optional[int] = None,
    filterset: Optional[SiteQuery] = None,
) -> "Tuple[int, Union[models.Site, Query[models.Site]]]":
    qs = db.query(models.Site).filter(models.Site.owner_id == owner.id)
    if filterset is not None:
        qs = filterset.apply_filters(qs)

    if site_id is not None:
        site = qs.filter(models.Site.id == site_id).one()
        return (1, site)
    else:
        assert paginator is not None
        count = qs.group_by(models.Site.id).count()
        return count, paginator.paginate(qs)


def create(db: Session, owner: schemas.UserOut, site: schemas.SiteCreate) -> models.Site:
    site_id = db.query(models.Site.id).filter_by(hostname=site.hostname, path=site.path.as_posix()).scalar()  # type: ignore
    if site_id is not None:
        raise ValidationError("A site with this hostname and path already exists")
    new_site = models.Site(
        **jsonable_encoder(site),
        creation_date=datetime.utcnow(),
        last_refresh=datetime.utcnow(),
        owner_id=owner.id,
    )
    db.add(new_site)
    db.flush()
    return new_site


def update(db: Session, owner: schemas.UserOut, site_id: int, update_data: Dict[str, Any]) -> models.Site:
    qs = db.query(models.Site).filter(models.Site.owner_id == owner.id, models.Site.id == site_id)
    site_in_db = qs.one()
    qs.update(jsonable_encoder(update_data))
    db.flush()
    return site_in_db


def delete(db: Session, owner: schemas.UserOut, site_id: int) -> None:
    site_in_db = db.query(models.Site).filter(models.Site.owner_id == owner.id, models.Site.id == site_id).one()
    db.delete(site_in_db)  # type: ignore
    db.flush()
