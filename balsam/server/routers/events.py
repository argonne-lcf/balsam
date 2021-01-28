from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import List

from fastapi import APIRouter, Depends, Query

from balsam import schemas
from balsam.server import settings
from balsam.server.models import BatchJob, Job, LogEvent, crud, get_session
from balsam.server.util import Paginator

router = APIRouter()
auth = settings.auth.get_auth_method()


class EventOrdering(str, Enum):
    timestamp = "timestamp"
    timestamp_desc = "-timestamp"


@dataclass
class EventQuery:
    job_id: List[int] = Query(None)
    batch_job_id: int = Query(None)
    scheduler_id: int = Query(None)
    tags: List[str] = Query(None)
    data: List[str] = Query(None)
    timestamp_before: datetime = Query(None)
    timestamp_after: datetime = Query(None)
    from_state: str = Query(None)
    to_state: str = Query(None)
    ordering: EventOrdering = Query("-timestamp")

    def apply_filters(self, qs):
        if self.job_id:
            qs = qs.filter(Job.id.in_(self.job_id))
        if self.batch_job_id or self.scheduler_id:
            qs = qs.join(BatchJob)
        if self.batch_job_id:
            qs = qs.filter(Job.batch_job_id == self.batch_job_id)
        if self.scheduler_id:
            qs = qs.filter(BatchJob.scheduler_id == self.scheduler_id)
        if self.tags:
            tags_dict = dict(t.split(":", 1) for t in self.tags if ":" in t)
            qs = qs.filter(Job.tags.contains(tags_dict))
        if self.data:
            data_dict = dict(d.split(":", 1) for d in self.data if ":" in d)
            qs = qs.filter(LogEvent.data.contains(data_dict))
        if self.timestamp_before:
            qs = qs.filter(LogEvent.timestamp <= self.timestamp_before)
        if self.timestamp_after:
            qs = qs.filter(LogEvent.timestamp >= self.timestamp_after)
        if self.from_state:
            qs = qs.filter(LogEvent.from_state == self.from_state)
        if self.to_state:
            qs = qs.filter(LogEvent.to_state == self.to_state)
        if self.ordering:
            desc = self.ordering.startswith("-")
            order_col = getattr(LogEvent, self.ordering.lstrip("-"))
            qs = qs.order_by(order_col.desc() if desc else order_col)
        return qs


@router.get("/", response_model=schemas.PaginatedLogEventOut)
def list(
    db=Depends(get_session),
    user=Depends(auth),
    paginator=Depends(Paginator),
    q=Depends(EventQuery),
):
    count, events = crud.events.fetch(db, owner=user, paginator=paginator, filterset=q)
    return {"count": count, "results": events}
