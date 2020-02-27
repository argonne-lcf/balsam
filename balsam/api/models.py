from datetime import datetime
import pathlib
import pytz
from typing import Union, List, Tuple, Dict
from pydantic import validator, root_validator
from enum import Enum
from .base_model import BalsamModel
from .query import Manager


def utc_datetime():
    return datetime.utcnow().replace(tzinfo=pytz.UTC)


class TransferItem(BalsamModel):
    pass


class Job(BalsamModel):
    name: str
    workflow: str
    num_nodes: int
    cpu_affinity = "depth"

    def history(self):
        return EventLog.objects.filter(job=self.pk)


class JobManager(Manager):
    model_class = Job
    bulk_create_enabled = True
    bulk_update_enabled = True
    bulk_delete_enabled = True


class SiteStatus(BalsamModel):
    num_nodes: int = 0
    num_idle_nodes: int = 0
    num_busy_nodes: int = 0
    num_down_nodes: int = 0
    backfill_windows: List[Tuple[int, int]] = [(0, 0)]
    queued_jobs: List[Tuple[int, int, str]] = [(0, 0, "")]


class Site(BalsamModel):
    pk: Union[int, None] = None
    hostname: str
    path: pathlib.Path
    last_refresh: datetime = None
    status: SiteStatus = SiteStatus
    apps: List[str] = [""]

    @validator("last_refresh", pre=True, always=True)
    def default_refresh(cls, v):
        return v or utc_datetime()


class SiteManager(Manager):
    model_class = Site


class AppBackend(BalsamModel):
    site_hostname: str
    site_path: pathlib.Path
    site: Union[Site, int]
    class_name: str

    @root_validator(pre=True)
    def validate_site(cls, values):
        site = values["site"]
        if isinstance(site, Site):
            values["site_hostname"] = site.hostname
            values["site_path"] = site.path
            values["site"] = site.pk
        return values


class App(BalsamModel):
    pk: Union[int, None] = None
    name: str
    description: str = ""
    parameters: List[str] = []
    owner: str = ""
    users: List[str] = []
    backends: List[AppBackend]

    @validator("backends")
    def validate_backends(cls, v):
        if not isinstance(v, list):
            v = [v]
        return v


class AppManager(Manager):
    model_class = App

    def merge(self, app_list, name, description=""):
        pks = [app.pk for app in app_list]
        resp = self.resource.merge(
            name=name, description=description, existing_apps=pks
        )
        return self._from_dict(resp)


class BatchState(str, Enum):
    pending_submission = "pending-submission"
    submit_failed = "submit-failed"
    queued = "queued"
    starting = "starting"
    running = "running"
    exiting = "exiting"
    finished = "finished"
    dep_hold = "dep-hold"
    user_hold = "user-hold"
    pending_deletion = "pending-deletion"


class BatchJob(BalsamModel):
    pk: Union[int, None]
    site: int
    project: str
    queue: str
    num_nodes: int
    wall_time_min: int
    job_mode: str
    filter_tags: Dict[str, str] = {}
    state: BatchState = BatchState.pending_submission
    scheduler_id: Union[int, None] = None
    status_message: str = ""
    start_time: Union[datetime, None] = None
    end_time: Union[datetime, None] = None
    revert: bool = False

    def jobs(self):
        return Job.objects.filter(batch_job_id=self.pk)


class BatchJobManager(Manager):
    model_class = BatchJob
    bulk_update_enabled = True


class NodeResources(BalsamModel):
    max_jobs_per_node: int
    max_wall_time_min: int
    running_job_counts: List[int]
    node_occupancies: List[float]
    idle_cores: List[int]
    idle_gpus: List[int]


class Session(BalsamModel):
    pk: Union[int, None]
    heartbeat: datetime = None
    label: str = ""
    site: int
    batch_job: Union[int, None] = None

    @validator("site", pre=True, always=True)
    def set_site_id(cls, v):
        if isinstance(v, Site):
            return v.pk
        return v

    @validator("batch_job", pre=True, always=True)
    def set_batch_job_id(cls, v):
        if isinstance(v, BatchJob):
            return v.pk
        return v

    @validator("heartbeat", pre=True, always=True)
    def default_time(cls, v):
        return v or utc_datetime()

    def acquire_jobs(
        self,
        acquire_unbound,
        states,
        max_num_acquire,
        filter_tags=None,
        node_resources=None,
        order_by=None,
    ):
        if node_resources is not None:
            node_resources = (
                node_resources.dict()
                if isinstance(node_resources, NodeResources)
                else NodeResources(**node_resources).dict()
            )
        return self.__class__.objects._do_acquire(
            self,
            acquire_unbound=acquire_unbound,
            states=states,
            max_num_acquire=max_num_acquire,
            filter_tags=filter_tags,
            node_resources=node_resources,
            order_by=order_by,
        )


class SessionManager(Manager):
    model_class = Session

    def _do_acquire(self, instance, **kwargs):
        acquired_raw = self.resource.acquire_jobs(uri=instance.pk, **kwargs)
        return Job.objects._unpack_list_response(acquired_raw)


class EventLog(BalsamModel):
    job_id: int
    from_state: str
    to_state: str
    timestamp: datetime
    message: str


class EventLogManager(Manager):
    model_class = EventLog
