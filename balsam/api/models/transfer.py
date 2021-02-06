from balsam import schemas
from balsam.api.manager import Manager
from balsam.api.model import BalsamModel


class TransferItem(BalsamModel):
    _create_model_cls = None
    _update_model_cls = schemas.TransferItemUpdate
    _read_model_cls = schemas.TransferItemOut


class TransferItemManager(Manager):
    path = "transfers/"
    _bulk_update_enabled = True
    _model_class = TransferItem
