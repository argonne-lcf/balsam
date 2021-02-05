from balsam import schemas
from balsam.api.manager_base import Manager
from balsam.api.model_base import BalsamModel


class TransferItem(BalsamModel):
    create_model_cls = None
    update_model_cls = schemas.TransferItemUpdate
    read_model_cls = schemas.TransferItemOut


class TransferItemManager(Manager):
    path = "transfers/"
    bulk_update_enabled = True
    model_class = TransferItem
