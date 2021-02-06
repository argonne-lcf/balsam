from balsam import schemas
from balsam.api.manager import Manager
from balsam.api.model import BalsamModel


class EventLog(BalsamModel):
    _create_model_cls = None
    _update_model_cls = None
    _read_model_cls = schemas.LogEventOut


class EventLogManager(Manager):
    path = "events/"
    _model_class = EventLog
