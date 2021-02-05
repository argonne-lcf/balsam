from balsam import schemas
from balsam.api.model_base import BalsamModel


class SiteBase(BalsamModel):
    create_model_cls = schemas.SiteCreate
    update_model_cls = schemas.SiteUpdate
    read_model_cls = schemas.SiteOut


class SiteManagerMixin:
    path = "sites/"
