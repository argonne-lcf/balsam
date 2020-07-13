from fastapi.encoders import jsonable_encoder
from balsamapi import models, ValidationError
from datetime import datetime


def fetch(
    db, owner, paginator=None, host_contains=None, path_contains=None, site_id=None
):
    qs = db.query(models.Site).filter(models.Site.owner_id == owner.id)
    if host_contains:
        qs = qs.filter(models.Site.hostname.like(f"%{host_contains}%"))
    if path_contains:
        qs = qs.filter(models.Site.path.like(f"%{path_contains}%"))
    if site_id is not None:
        qs = qs.filter(models.Site.id == site_id).one()
    return paginator.paginate(qs) if site_id is None else qs


def create(db, owner, site):
    if (
        db.query(models.Site.id)
        .filter_by(hostname=site.hostname, path=site.path.as_posix())
        .scalar()
        is not None
    ):
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


def update(db, owner, site_id, update_data):
    qs = db.query(models.Site).filter(
        models.Site.owner_id == owner.id, models.Site.id == site_id
    )
    site_in_db = qs.one()
    qs.update(jsonable_encoder(update_data))
    db.flush()
    return site_in_db


def delete(db, owner, site_id):
    site_in_db = (
        db.query(models.Site)
        .filter(models.Site.owner_id == owner.id, models.Site.id == site_id)
        .one()
    )
    db.delete(site_in_db)
    db.flush()
