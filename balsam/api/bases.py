from balsam import schemas

from .model import BalsamModel


class SiteBase(BalsamModel):
    _create_model_cls = schemas.SiteCreate
    _update_model_cls = schemas.SiteUpdate
    _read_model_cls = schemas.SiteOut


class SiteManagerMixin:
    _api_path = "sites/"
