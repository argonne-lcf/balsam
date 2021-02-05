from typing import Dict

from balsam import schemas
from balsam.api.manager_base import Manager
from balsam.api.model_base import BalsamModel

AppParameter = schemas.AppParameter
TransferSlot = schemas.TransferSlot


class App(BalsamModel):
    create_model_cls = schemas.AppCreate
    update_model_cls = schemas.AppUpdate
    read_model_cls = schemas.AppOut

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
    model_class = App
