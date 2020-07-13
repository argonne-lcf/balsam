from fastapi.encoders import jsonable_encoder
from balsam.server import models, ValidationError
from sqlalchemy.exc import IntegrityError


def fetch(db, owner, paginator=None, app_id=None, filter_site_ids=None):
    qs = db.query(models.App).join(models.Site).filter(models.Site.owner_id == owner.id)
    if filter_site_ids:
        qs = qs.filter(models.App.site_id.in_(filter_site_ids))
    if app_id is not None:
        qs = qs.filter(models.App.id == app_id).one()
    return paginator.paginate(qs) if app_id is None else qs


def flush_or_400(db):
    try:
        db.flush()
    except IntegrityError as e:
        if "duplicate key value" in str(e):
            db.rollback()
            raise ValidationError(
                "An app with this site_id and class_path already exists"
            )
        else:
            raise


def create(db, owner, app):
    # Will raise if the owner does not have a matching Site ID
    app.site_id = (
        db.query(models.Site.id).filter_by(id=app.site_id, owner_id=owner.id).one()[0]
    )
    new_app = models.App(**jsonable_encoder(app))
    db.add(new_app)
    flush_or_400(db)
    return new_app


def update(db, owner, app_id, update_data):
    qs = (
        db.query(models.App)
        .join(models.Site, models.App.site_id == models.Site.id)
        .filter(models.Site.owner_id == owner.id, models.App.id == app_id)
    )
    app_in_db = qs.one()
    if "site_id" in update_data:
        update_data["site_id"] = (
            db.query(models.Site.id)
            .filter_by(id=update_data["site_id"], owner_id=owner.id)
            .one()
        )
    for k, v in update_data.items():
        setattr(app_in_db, k, v)
    flush_or_400(db)
    return app_in_db


def delete(db, owner, app_id):
    app_in_db = (
        db.query(models.App)
        .join(models.Site)
        .filter(models.Site.owner_id == owner.id, models.App.id == app_id)
        .one()
    )
    db.delete(app_in_db)
    db.flush()
