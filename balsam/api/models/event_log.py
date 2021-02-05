from balsam import schemas
from balsam.api.manager_base import Manager
from balsam.api.model_base import BalsamModel


class EventLog(BalsamModel):
    create_model_cls = None
    update_model_cls = None
    read_model_cls = schemas.LogEventOut


class EventLogManager(Manager):
    path = "events/"
    model_class = EventLog
