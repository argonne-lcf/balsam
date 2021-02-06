from typing import Dict

from balsam import schemas
from balsam.api.manager import Manager
from balsam.api.model import BalsamModel

AppParameter = schemas.AppParameter
TransferSlot = schemas.TransferSlot


class App(BalsamModel):
    _create_model_cls = schemas.AppCreate
    _update_model_cls = schemas.AppUpdate
    _read_model_cls = schemas.AppOut

    def __init__(
        self,
        site_id: int = None,
        class_path: str = None,
        parameters: Dict[str, AppParameter] = None,
        transfers: Dict[str, TransferSlot] = None,
        description: str = "",
        **kwargs,
    ):
        if transfers is None:
            transfers = {}
        return super().__init__(
            site_id=site_id,
            class_path=class_path,
            parameters=parameters if parameters else {},
            transfers=transfers,
            description=description,
            **kwargs,
        )


class AppManager(Manager):
    path = "apps/"
    _model_class = App
