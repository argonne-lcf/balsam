from typing import Any, Dict, Optional, Tuple, cast

from fastapi.encoders import jsonable_encoder
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Query, Session

from balsam import schemas
from balsam.server import ValidationError, models
from balsam.server.routers.filters import AppQuery
from balsam.server.utils import Paginator


def fetch(
    db: Session,
    owner: schemas.UserOut,
    paginator: Optional[Paginator[models.App]] = None,
    app_id: Optional[int] = None,
    filterset: Optional[AppQuery] = None,
) -> "Tuple[int, Query[models.App]]":
    qs = db.query(models.App).join(models.Site).filter(models.Site.owner_id == owner.id)  # type: ignore
    if filterset is not None:
        qs = filterset.apply_filters(qs)
    if app_id is not None:
        qs = qs.filter(models.App.id == app_id).one()
        return (1, qs)
    else:
        assert paginator is not None
        count = qs.group_by(models.App.id).count()
        return count, paginator.paginate(qs)


def flush_or_400(db: Session) -> None:
    try:
        db.flush()
    except IntegrityError as e:
        if "duplicate key value" in str(e):
            db.rollback()
            raise ValidationError("An app with this site_id and class_path already exists")
        else:
            raise


def create(db: Session, owner: schemas.UserOut, app: schemas.AppCreate) -> models.App:
    # Will raise if the owner does not have a matching Site ID
    app.site_id = db.query(models.Site.id).filter_by(id=app.site_id, owner_id=owner.id).one()[0]
    new_app = models.App(**jsonable_encoder(app))
    db.add(new_app)
    flush_or_400(db)
    return new_app


def update(db: Session, owner: schemas.UserOut, app_id: int, update_data: Dict[str, Any]) -> models.App:
    qs = (
        db.query(models.App)
        .join(models.Site, models.App.site_id == models.Site.id)  # type: ignore
        .filter(models.Site.owner_id == owner.id, models.App.id == app_id)
    )
    app_in_db = cast(models.App, qs.one())
    if "site_id" in update_data:
        update_data["site_id"] = (
            db.query(models.Site.id).filter_by(id=update_data["site_id"], owner_id=owner.id).one()
        )
    for k, v in update_data.items():
        setattr(app_in_db, k, v)
    flush_or_400(db)
    return app_in_db


def delete(db: Session, owner: schemas.UserOut, app_id: int) -> None:
    app_in_db = (
        db.query(models.App).join(models.Site).filter(models.Site.owner_id == owner.id, models.App.id == app_id).one()  # type: ignore
    )
    db.delete(app_in_db)  # type: ignore
    db.flush()
