from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Set, Tuple, cast

from fastapi import Query
from sqlalchemy import orm

from balsam import schemas
from balsam.server.models import App, BatchJob, Job, LogEvent, Session, Site, TransferItem


@dataclass
class SiteQuery:
    hostname: str = Query(None)
    path: str = Query(None)
    id: List[int] = Query(None)

    def apply_filters(self, qs: "orm.Query[Site]") -> "orm.Query[Site]":
        if self.hostname:
            qs = qs.filter(Site.hostname.like(f"%{self.hostname}%"))
        if self.path:
            qs = qs.filter(Site.path.like(f"%{self.path}%"))
        if self.id:
            qs = qs.filter(Site.id.in_(self.id))
        return qs


@dataclass
class AppQuery:
    site_id: List[int] = Query(None)
    id: List[int] = Query(None)
    class_path: str = Query(None)

    def apply_filters(self, qs: "orm.Query[App]") -> "orm.Query[App]":
        if self.site_id:
            qs = qs.filter(App.site_id.in_(self.site_id))
        if self.id:
            qs = qs.filter(App.id.in_(self.id))
        if self.class_path is not None:
            qs = qs.filter(App.class_path == self.class_path)
        return qs


@dataclass
class SessionQuery:
    def apply_filters(self, qs: "orm.Query[Session]") -> "orm.Query[Session]":
        return qs


@dataclass
class EventLogQuery:
    job_id: List[int] = Query(None)
    batch_job_id: int = Query(None)
    scheduler_id: int = Query(None)
    tags: List[str] = Query(None)
    data: List[str] = Query(None)
    timestamp_before: datetime = Query(None)
    timestamp_after: datetime = Query(None)
    from_state: str = Query(None)
    to_state: str = Query(None)
    ordering: schemas.EventOrdering = Query("-timestamp")

    def apply_filters(self, qs: "orm.Query[LogEvent]") -> "orm.Query[LogEvent]":
        if self.job_id:
            qs = qs.filter(Job.id.in_(self.job_id))
        if self.batch_job_id or self.scheduler_id:
            qs = qs.join(BatchJob)  # type: ignore
        if self.batch_job_id:
            qs = qs.filter(Job.batch_job_id == self.batch_job_id)
        if self.scheduler_id:
            qs = qs.filter(BatchJob.scheduler_id == self.scheduler_id)
        if self.tags:
            tags_dict: Dict[str, str] = dict(t.split(":", 1) for t in self.tags if ":" in t)  # type: ignore
            qs = qs.filter(Job.tags.contains(tags_dict))  # type: ignore
        if self.data:
            data_dict: Dict[str, str] = dict(d.split(":", 1) for d in self.data if ":" in d)  # type: ignore
            qs = qs.filter(LogEvent.data.contains(data_dict))  # type: ignore
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


@dataclass
class JobQuery:
    id: List[int] = Query(None, min_items=1)
    parent_id: List[int] = Query(None, min_items=1)
    app_id: int = Query(None)
    site_id: int = Query(None)
    batch_job_id: int = Query(None)
    last_update_before: datetime = Query(None)
    last_update_after: datetime = Query(None)
    workdir__contains: str = Query(None)
    state__ne: schemas.JobState = Query(None)
    state: Set[schemas.JobState] = Query(None)
    tags: List[str] = Query(None)
    parameters: List[str] = Query(None)
    ordering: schemas.JobOrdering = Query(None)

    def apply_filters(self, qs: "orm.Query[Job]") -> "orm.Query[Job]":
        if self.id:
            qs = qs.filter(Job.id.in_(self.id))
        if self.parent_id:
            qs = qs.filter(Job.parents.any(Job.id.in_(self.parent_id)))
        if self.app_id:
            qs = qs.filter(Job.app_id == self.app_id)
        if self.site_id:
            qs = qs.filter(Site.id == self.site_id)
        if self.batch_job_id:
            qs: "orm.Query[Job]" = qs.join(BatchJob)  # type: ignore
            qs = qs.filter(BatchJob.id == self.batch_job_id)
        if self.last_update_before:
            qs = qs.filter(Job.last_update <= self.last_update_before)
        if self.last_update_after:
            qs = qs.filter(Job.last_update >= self.last_update_after)
        if self.workdir__contains:
            qs = qs.filter(Job.workdir.like(f"%{self.workdir__contains}%"))
        if self.state__ne:
            qs = qs.filter(Job.state != self.state__ne)
        if self.state:
            qs = qs.filter(Job.state.in_(self.state))
        if self.tags:
            tags_dict: Dict[str, str] = dict(t.split(":", 1) for t in self.tags if ":" in t)  # type: ignore
            qs = qs.filter(Job.tags.contains(tags_dict))  # type: ignore
        if self.parameters:
            params_dict: Dict[str, str] = dict(p.split(":", 1) for p in self.parameters if ":" in p)  # type: ignore
            qs = qs.filter(Job.parameters.contains(params_dict))  # type: ignore
        if self.ordering:
            desc = self.ordering.startswith("-")
            order_col = getattr(Job, self.ordering.lstrip("-"))
            qs = qs.order_by(order_col.desc() if desc else order_col)
        return qs


@dataclass
class TransferItemQuery:
    id: List[int] = Query(None)
    site_id: int = Query(None)
    job_id: List[int] = Query(None)
    state: Set[schemas.TransferItemState] = Query(None)
    direction: schemas.TransferDirection = Query(None)
    job_state: str = Query(None)
    tags: List[str] = Query(None)

    def apply_filters(self, qs: "orm.Query[TransferItem]") -> "orm.Query[TransferItem]":
        if self.id:
            qs = qs.filter(TransferItem.id.in_(self.id))
        if self.site_id:
            qs = qs.filter(Site.id == self.site_id)
        if self.job_id:
            qs = qs.filter(Job.id.in_(self.job_id))
        if self.state:
            qs = qs.filter(TransferItem.state.in_(self.state))
        if self.direction:
            qs = qs.filter(TransferItem.direction == self.direction)
        if self.job_state:
            qs = qs.filter(Job.state == self.job_state)
        if self.tags:
            tags_dict = dict(cast(Tuple[str, str], t.split(":", 1)) for t in self.tags if ":" in t)
            qs = qs.filter(Job.tags.contains(tags_dict))  # type: ignore
        return qs


@dataclass
class BatchJobQuery:
    site_id: List[int] = Query(None)
    state: List[str] = Query(None)
    scheduler_id: int = Query(None)
    queue: str = Query(None)
    ordering: schemas.BatchJobOrdering = Query(None)
    start_time_before: datetime = Query(None)
    start_time_after: datetime = Query(None)
    end_time_before: datetime = Query(None)
    end_time_after: datetime = Query(None)
    filter_tags: List[str] = Query(None)

    def apply_filters(self, qs: "orm.Query[BatchJob]") -> "orm.Query[BatchJob]":
        if self.site_id:
            qs = qs.filter(BatchJob.site_id.in_(self.site_id))
        if self.state:
            qs = qs.filter(BatchJob.state.in_(self.state))
        if self.scheduler_id:
            qs = qs.filter(BatchJob.scheduler_id == self.scheduler_id)
        if self.queue:
            qs = qs.filter(BatchJob.queue == self.queue)

        if self.start_time_before:
            qs = qs.filter(BatchJob.start_time <= self.start_time_before)
        if self.start_time_after:
            qs = qs.filter(BatchJob.start_time >= self.start_time_after)

        if self.end_time_before:
            qs = qs.filter(BatchJob.end_time <= self.end_time_before)
        if self.end_time_after:
            qs = qs.filter(BatchJob.end_time >= self.end_time_after)

        if self.filter_tags:
            tags_dict: Dict[str, str] = dict(t.split(":", 1) for t in self.filter_tags if ":" in t)  # type: ignore
            qs = qs.filter(BatchJob.filter_tags.contains(tags_dict))  # type: ignore
        if self.ordering:
            desc = self.ordering.startswith("-")
            order_col = getattr(BatchJob, self.ordering.lstrip("-"))
            qs = qs.order_by(order_col.desc() if desc else order_col)
        return qs
