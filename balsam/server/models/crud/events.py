from typing import Tuple

from sqlalchemy.orm import Query, Session

from balsam import schemas
from balsam.server import models
from balsam.server.routers.filters import EventLogQuery
from balsam.server.utils import Paginator


def fetch(
    db: Session, owner: schemas.UserOut, paginator: Paginator[models.LogEvent], filterset: EventLogQuery
) -> "Tuple[int, Query[models.LogEvent]]":
    qs = db.query(models.LogEvent).join(models.Job).join(models.App).join(models.Site)  # type: ignore
    qs = qs.filter(models.Site.owner_id == owner.id)
    qs = filterset.apply_filters(qs)
    count = qs.group_by(models.LogEvent.id).count()
    events = paginator.paginate(qs)
    return count, events
