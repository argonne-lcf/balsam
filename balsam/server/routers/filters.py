from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Set, Tuple, cast

from fastapi import Query
from sqlalchemy import orm

from balsam import schemas
from balsam.server.models import App, BatchJob, Job, LogEvent, Session, Site, TransferItem


@dataclass
class SiteQuery:
    hostname: str = Query(None, description="Only return Sites with hostnames containing this string.")
    path: str = Query(None, description="Only return Sites with paths containing this string.")
    id: List[int] = Query(None, description="Only return Sites having an id in this list.")
    last_refresh_after: datetime = Query(None, description="Only return Sites active since this time (UTC)")

    def apply_filters(self, qs: "orm.Query[Site]") -> "orm.Query[Site]":
        if self.hostname:
            qs = qs.filter(Site.hostname.like(f"%{self.hostname}%"))
        if self.path:
            qs = qs.filter(Site.path.like(f"%{self.path}%"))
        if self.id:
            qs = qs.filter(Site.id.in_(self.id))
        if self.last_refresh_after:
            qs = qs.filter(Site.last_refresh >= self.last_refresh_after)
        return qs


@dataclass
class AppQuery:
    site_id: List[int] = Query(None, description="Only return Apps associated with the Site IDs in this list.")
    id: List[int] = Query(None, description="Only return Apps with IDs in this list.")
    class_path: str = Query(None, description="Only return Apps matching this dotted class path (module.ClassName)")
    site_path: str = Query(None, description="Only return Apps from Sites having paths containing this substring.")

    def apply_filters(self, qs: "orm.Query[App]") -> "orm.Query[App]":
        if self.site_id:
            qs = qs.filter(App.site_id.in_(self.site_id))
        if self.id:
            qs = qs.filter(App.id.in_(self.id))
        if self.class_path is not None:
            qs = qs.filter(App.class_path == self.class_path)
        if self.site_path:
            qs = qs.filter(Site.path.like(f"%{self.site_path}%"))
        return qs


@dataclass
class SessionQuery:
    id: List[int] = Query(None, description="Only return Sessions having an id in this list.")

    def apply_filters(self, qs: "orm.Query[Session]") -> "orm.Query[Session]":
        if self.id:
            qs = qs.filter(Session.id.in_(self.id))
        return qs


@dataclass
class EventLogQuery:
    id: List[int] = Query(None, description="Only return EventLogs having an id in this list.")
    job_id: List[int] = Query(None, description="Only return Events associated with Job IDs in this list.")
    batch_job_id: int = Query(None, description="Only return Events associated this BatchJob id.")
    scheduler_id: int = Query(None, description="Only return Events associated with this HPC scheduler job ID.")
    tags: List[str] = Query(
        None, description="Only return Events for Jobs containing these tags (list of KEY:VALUE strings)"
    )
    data: List[str] = Query(None, description="Only return Events containing this data (list of KEY:VALUE strings)")
    timestamp_before: datetime = Query(None, description="Only return Events before this time (UTC).")
    timestamp_after: datetime = Query(None, description="Only return Events that occured after this time (UTC).")
    from_state: str = Query(None, description="Only return Events transitioning from this Job state.")
    to_state: str = Query(None, description="Only return Events transitioning to this Job state.")
    ordering: schemas.EventOrdering = Query("-timestamp", description="Order events by this field.")

    def apply_filters(self, qs: "orm.Query[LogEvent]") -> "orm.Query[LogEvent]":
        if self.id:
            qs = qs.filter(LogEvent.id.in_(self.id))
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
    id: List[int] = Query(None, min_items=1, description="Only return Jobs with ids in this list.")
    parent_id: List[int] = Query(
        None, min_items=1, description="Only return Jobs that are children of Jobs with ids in this list."
    )
    app_id: int = Query(None, description="Only return Jobs associated with this App id.")
    site_id: List[int] = Query(None, description="Only return Jobs associated with these Site ids.")
    batch_job_id: int = Query(None, description="Only return Jobs associated with this BatchJob id.")
    last_update_before: datetime = Query(
        None, description="Only return Jobs that were updated before this time (UTC)."
    )
    last_update_after: datetime = Query(None, description="Only return Jobs that were updated after this time (UTC).")
    workdir__contains: str = Query(None, description="Only return jobs with workdirs containing this string.")
    state__ne: schemas.JobState = Query(None, description="Only return jobs with states not equal to this state.")
    state: Set[schemas.JobState] = Query(None, description="Only return jobs in this set of states.")
    tags: List[str] = Query(None, description="Only return jobs containing these tags (list of KEY:VALUE strings)")
    parameters: List[str] = Query(
        None, description="Only return jobs having these App command parameters (list of KEY:VALUE strings)"
    )
    pending_file_cleanup: bool = Query(None, description="Only return jobs which have not yet had workdir cleaned.")
    ordering: schemas.JobOrdering = Query(None, description="Order Jobs by this field.")

    def apply_filters(self, qs: "orm.Query[Job]") -> "orm.Query[Job]":  # noqa: C901
        if self.id:
            qs = qs.filter(Job.id.in_(self.id))
        if self.parent_id:
            qs = qs.filter(Job.parents.any(Job.id.in_(self.parent_id)))
        if self.app_id:
            qs = qs.filter(Job.app_id == self.app_id)
        if self.site_id:
            qs = qs.filter(Site.id.in_(self.site_id))
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
        if self.pending_file_cleanup:
            qs = qs.filter(Job.pending_file_cleanup)
        if self.ordering:
            desc = self.ordering.startswith("-")
            order_col = getattr(Job, self.ordering.lstrip("-"))
            qs = qs.order_by(order_col.desc() if desc else order_col)
        return qs


@dataclass
class TransferItemQuery:
    id: List[int] = Query(None, description="Only return transfer items with IDs in this list.")
    site_id: int = Query(None, description="Only return transfer items associated with this Site id.")
    job_id: List[int] = Query(None, description="Only return transfer items associated with this Job id list.")
    state: Set[schemas.TransferItemState] = Query(
        None, description="Only return transfer items in this set of states."
    )
    direction: schemas.TransferDirection = Query(None, description="Only return items in this transfer direction.")
    job_state: str = Query(None, description="Only return transfer items for Jobs having this state.")
    tags: List[str] = Query(
        None, description="Only return transfer items for Jobs having these tags (list of KEY:VALUE strings)."
    )

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
    id: List[int] = Query(None, description="Only return BatchJobs having an id in this list.")
    site_id: List[int] = Query(None, description="Only return batchjobs for Sites in this id list.")
    state: List[str] = Query(None, description="Only return batchjobs having one of these States in this list.")
    scheduler_id: int = Query(None, description="Return the batchjob with this local scheduler id.")
    queue: str = Query(None, description="Only return batchjobs submitted to this queue.")
    ordering: schemas.BatchJobOrdering = Query(None, description="Order batchjobs by this field.")
    start_time_before: datetime = Query(
        None, description="Only return batchjobs that started before this time (UTC)."
    )
    start_time_after: datetime = Query(None, description="Only return batchjobs that started after this time (UTC).")
    end_time_before: datetime = Query(None, description="Only return batchjobs that finished before this time (UTC).")
    end_time_after: datetime = Query(None, description="Only return batchjobs that finished after this time (UTC).")
    filter_tags: List[str] = Query(
        None, description="Only return batchjobs processing these tags (list of KEY:VALUE strings)."
    )

    def apply_filters(self, qs: "orm.Query[BatchJob]") -> "orm.Query[BatchJob]":
        if self.id:
            qs = qs.filter(BatchJob.id.in_(self.id))
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
