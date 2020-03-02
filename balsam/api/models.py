from datetime import datetime
import pathlib
import pytz
from typing import Union, List, Tuple, Dict, Optional
from pydantic import validator, root_validator, AnyUrl
from enum import Enum
from uuid import UUID
from .base_model import BalsamModel


def utc_datetime():
    return datetime.utcnow().replace(tzinfo=pytz.UTC)


class TransferState(str, Enum):
    pending = "pending"
    active = "active"
    finished = "finished"
    failed = "failed"


ALLOWED_TRANSFER_PROTOCOLS = ["globus", "scp", "rsync"]


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


class TransferItem(BalsamModel):
    pk: Optional[int] = None
    direction: str
    source: Union[AnyUrl, pathlib.Path]
    destination: Union[AnyUrl, pathlib.Path]
    state: TransferState = TransferState.pending
    task_id: Union[UUID, int, None] = None
    status_message: str = ""
    job: Optional[int] = None
    protocol: str = ""
    remote_netloc: str = ""
    source_path: pathlib.Path = ""
    destination_path: pathlib.Path = ""

    @validator("direction", pre=True)
    def in_or_out(cls, v):
        if v not in ["in", "out"]:
            raise ValueError("direction must be 'in' or 'out'")
        return v

    @root_validator(pre=True)
    def validate_incoming(cls, values):
        """
        The API stores and returns (protocol, remote_netloc, source_path, destination_path, job)
        If we are receiving data, we populate `source` and `destination` URL from these fields
        """
        direction = values.get("direction")
        protocol = values.get("protocol", "")
        remote_netloc = values.get("remote_netloc", "")
        source_path = values.get("source_path", "")
        dest_path = values.get("destination_path", "")

        if not (values.get("source") and values.get("destination")):
            if direction == "in":
                source = f"{protocol}://{remote_netloc}{source_path}"
                dest = dest_path
            else:
                source = source_path
                dest = f"{protocol}://{remote_netloc}{dest_path}"
            values["source"] = source
            values["destination"] = dest
        return values

    @root_validator(pre=False)
    def validate_source_dest(cls, values):
        """
        Validate `source` and `destination` URL/Path and make the other fields consistent.
        """
        direction = values.get("direction")
        source = values["source"]
        dest = values["destination"]
        if direction == "in":
            cls.validate_remote_url(source, values)
            cls.validate_rel_path(dest, values)
            values["source_path"] = source.path
            values["destination_path"] = dest
        else:
            cls.validate_remote_url(dest, values)
            cls.validate_rel_path(source, values)
            values["source_path"] = source
            values["destination_path"] = dest.path
        return values

    @staticmethod
    def validate_remote_url(v, values):
        assert isinstance(v, AnyUrl), f"Invalid remote URL: {v}"
        assert (
            v.scheme in ALLOWED_TRANSFER_PROTOCOLS
        ), f"Invalid transfer scheme: {v.scheme}"
        assert v.path, f"Remote URL must end with a filesystem path: {v}"
        values["protocol"] = v.scheme
        values["remote_netloc"] = v.host

    @staticmethod
    def validate_rel_path(v, values):
        assert isinstance(v, pathlib.Path), f"{v} is not a valid path"
        assert not v.is_absolute(), f"{v} must be a relative path"


class JobState(str, Enum):
    created = "CREATED"
    awaiting_parents = "AWAITING_PARENTS"
    ready = "READY"
    staged_in = "STAGED_IN"
    preprocessed = "PREPROCESSED"
    running = "RUNNING"
    run_done = "RUN_DONE"
    postprocessed = "POSTPROCESSED"
    job_finished = "JOB_FINISHED"
    run_error = "RUN_ERROR"
    run_timeout = "RUN_TIMEOUT"
    restart_ready = "RESTART_READY"
    failed = "FAILED"
    killed = "KILLED"
    reset = "RESET"


def rm_leading_underscore(s: str) -> str:
    return s.lstrip("_")


class Job(BalsamModel):
    class Config:
        extra = "ignore"
        validate_assignment = True
        alias_generator = rm_leading_underscore

    pk: Union[int, None]
    workdir: pathlib.Path
    app: Union[App, int]
    parameters: Dict[str, str]

    transfer_items: List[TransferItem] = []
    tags: Dict[str, str] = {}
    data: Dict = {}

    parents: List[int] = []

    # These hidden fields are read-only at the REST-API level
    # Add properties to enforce read-only access by users
    _children: List[int] = []
    _batch_job: Union[int, None] = None
    _app_name: str = None
    _site: str = None
    _app_class: str = None

    # Status
    return_code: Union[int, None] = None
    last_error: str = ""
    _lock_status: str = ""
    state: JobState = JobState.created
    state_timestamp: Union[datetime, None] = None
    state_message: str = ""
    _last_update: Union[datetime, None] = None

    # Resources
    num_nodes: int = 1
    ranks_per_node: int = 1
    threads_per_rank: int = 1
    threads_per_core: int = 1
    cpu_affinity: str = ""
    gpus_per_rank: int = 0
    node_packing_count: int = 1
    wall_time_min: int = 0

    @validator("app")
    def app_id(cls, v):
        if isinstance(v, App):
            return v.pk
        return v

    # We override the default constructor because only a subset of these fields
    # should actually be set by the client. The remaining fields can be set via
    # pass-through kwargs by internal Balsam components.
    def __init__(
        self,
        workdir,
        app,
        parameters,
        parents=None,
        transfer_items=None,
        tags=None,
        data=None,
        num_nodes=1,
        ranks_per_node=1,
        threads_per_rank=1,
        threads_per_core=1,
        cpu_affinity="",
        gpus_per_rank=0,
        node_packing_count=1,
        wall_time_min=0,
        **kwargs,
    ):
        d = locals()
        d.pop("kwargs")
        d.update(kwargs)
        d = {k: v for k, v in d.items() if v is not None}
        return super().__init__(**d)

    def history(self):
        return EventLog.objects.filter(job=self.pk)

    @property
    def children(self):
        return getattr(self, "_children")

    @property
    def batch_job(self):
        return getattr(self, "_batch_job")

    @property
    def app_name(self):
        return getattr(self, "_app_name")

    @property
    def site(self):
        return getattr(self, "_site")

    @property
    def app_class(self):
        return getattr(self, "_app_class")

    @property
    def lock_status(self):
        return getattr(self, "_lock_status")

    @property
    def last_update(self):
        return getattr(self, "_last_update")


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
    site: Union[int, Site]
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

    @validator("site")
    def site_pk(cls, v):
        if isinstance(v, Site):
            if not isinstance(v.pk, int):
                raise ValueError(
                    "Site does not have a pk assigned yet; does it need to be saved?"
                )
            return v.pk
        return int(v)

    def jobs(self):
        return Job.objects.filter(batch_job_id=self.pk)


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


class EventLog(BalsamModel):
    job_id: int
    from_state: str
    to_state: str
    timestamp: datetime
    message: str
