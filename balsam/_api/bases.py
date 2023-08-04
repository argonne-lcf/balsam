import concurrent.futures
import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, NamedTuple, Optional, Set, Type, Union

from balsam import schemas
from balsam.schemas import JobState, deserialize, raise_from_serialized, serialize

from .app import ApplicationDefinition
from .manager import Manager
from .model import CreatableBalsamModel, Field, NonCreatableBalsamModel

if TYPE_CHECKING:
    from balsam._api.models import (  # noqa: F401
        App,
        BatchJob,
        EventLog,
        Job,
        JobManager,
        JobQuery,
        Session,
        Site,
        TransferItem,
    )
    from balsam.client import RESTClient

JobTransferItem = schemas.JobTransferItem
RUNNABLE_STATES = schemas.RUNNABLE_STATES
DONE_STATES = schemas.DONE_STATES

logger = logging.getLogger(__name__)


class SiteBase(CreatableBalsamModel):
    _create_model_cls = schemas.SiteCreate
    _update_model_cls = schemas.SiteUpdate
    _read_model_cls = schemas.SiteOut


class SiteManagerBase(Manager["Site"]):
    _api_path = "sites/"


class AppBase(CreatableBalsamModel):
    _create_model_cls = schemas.AppCreate
    _update_model_cls = schemas.AppUpdate
    _read_model_cls = schemas.AppOut


class AppManagerBase(Manager["App"]):
    _api_path = "apps/"


class BatchJobBase(CreatableBalsamModel):
    _create_model_cls = schemas.BatchJobCreate
    _update_model_cls = schemas.BatchJobUpdate
    _read_model_cls = schemas.BatchJobOut
    # These fields overwritten by the generated subclass anyway
    # Used for type checking the methods below
    queue: Field[str]
    project: Field[str]
    num_nodes: Field[int]
    wall_time_min: Field[int]
    partitions: Field[Optional[List[schemas.BatchJobPartition]]]
    optional_params: Field[Dict[str, str]]

    def validate(
        self,
        allowed_queues: Dict[str, schemas.AllowedQueue],
        allowed_projects: List[str],
        optional_batch_job_params: Dict[str, str],
    ) -> None:
        if self.queue not in allowed_queues:
            raise ValueError(f"Unknown queue {self.queue} " f"(known: {list(allowed_queues.keys())})")
        queue = allowed_queues[self.queue]
        if self.num_nodes > queue.max_nodes:
            raise ValueError(f"{self.num_nodes} exceeds queue max num_nodes {queue.max_nodes}")
        if self.num_nodes < 1:
            raise ValueError("self size must be at least 1 node")
        if self.wall_time_min > queue.max_walltime:
            raise ValueError(f"{self.wall_time_min} exceeds queue max wall_time_min {queue.max_walltime}")

        if self.project not in allowed_projects:
            raise ValueError(f"Unknown project {self.project} " f"(known: {allowed_projects})")
        if self.partitions:
            if sum(part.num_nodes for part in self.partitions) != self.num_nodes:
                raise ValueError("Sum of partition sizes must equal batchjob num_nodes")

        extras = set(self.optional_params.keys())
        allowed_extras = set(optional_batch_job_params.keys())
        extraneous = extras.difference(allowed_extras)
        if extraneous:
            raise ValueError(f"Extraneous optional_params: {extraneous} " f"(allowed extras: {allowed_extras})")

    def partitions_to_cli_args(self) -> str:
        if not self.partitions:
            return ""
        args = ""
        for part in self.partitions:
            job_mode = part.job_mode
            num_nodes = part.num_nodes
            filter_tags = ":".join(f"{k}={v}" for k, v in part.filter_tags.items())
            args += f" --part {job_mode}:{num_nodes}"
            if filter_tags:
                args += f":{filter_tags}"
        return args


class BatchJobManagerBase(Manager["BatchJob"]):
    _api_path = "batch-jobs/"
    _bulk_update_enabled = True


AppDefType = Type[ApplicationDefinition]
InputAppType = Union[int, str, AppDefType]


class JobBase(CreatableBalsamModel):
    _create_model: Optional[schemas.JobCreate]
    _update_model: Optional[schemas.JobUpdate]
    _read_model: Optional[schemas.JobOut]
    _create_model_cls = schemas.JobCreate
    _update_model_cls = schemas.JobUpdate
    _read_model_cls = schemas.JobOut

    objects: "JobManager"
    app_id: Field[int]
    workdir: Field[Path]
    parent_ids: Field[Set[int]]
    state: Field[Optional[JobState]]

    class NoResult(ValueError):
        pass

    def __init__(self, app_id: InputAppType, site_name: Optional[str] = None, **kwargs: Any) -> None:
        app_id = self._resolve_app_id(app_id, site_name)
        super().__init__(**kwargs, app_id=app_id)

    @classmethod
    def _resolve_app_id(cls, app: InputAppType, site_name: Optional[str]) -> int:
        if isinstance(app, int):
            app_id = app
        elif isinstance(app, str):
            app = ApplicationDefinition.load_by_name(app, site_name)
            assert app.__app_id__ is not None
            app_id = app.__app_id__
        else:
            if app.__app_id__ is None:
                raise ValueError(
                    f"Cannot resolve ID from ApplicationDefinition {app}: __app_id__ is None. You need to app.sync() prior to creating Jobs with this app."
                )
            app_id = app.__app_id__
        return app_id

    @property
    def app(self) -> AppDefType:
        """
        Fetch the ApplicationDefinition class associated with this Job.
        """
        if self.app_id is None:
            raise ValueError("Cannot fetch by app ID; is None")
        return ApplicationDefinition.load_by_id(self.app_id)

    @property
    def site_id(self) -> int:
        """
        Fetch the Site ID associated with Job
        """
        if self.app_id is None:
            raise ValueError("Cannot fetch by app ID; is None")
        app_def = ApplicationDefinition.load_by_id(self.app_id)
        assert app_def._site_id is not None
        return app_def._site_id

    def get_parameters(self) -> Dict[str, Any]:
        """
        Unpack and return the Job parameters dictionary
        """
        if self._state == "clean":
            assert self._read_model is not None
            ser = self._read_model.serialized_parameters
        elif self._state == "creating":
            assert self._create_model is not None
            ser = self._create_model.serialized_parameters
        else:
            if "serialized_parameters" in self._dirty_fields:
                assert self._update_model is not None
                ser = self._update_model.serialized_parameters
            else:
                assert self._read_model is not None
                ser = self._read_model.serialized_parameters
        params: Dict[str, Any] = deserialize(ser)
        if not isinstance(params, dict):
            raise ValueError(f"Deserialized Job parameters are of type {type(params)}; must be dict.")
        return params

    def set_parameters(self, value: Dict[str, Any]) -> None:
        """
        Set the Job parameters dictionary
        """
        serialized = serialize(value)
        if self._state == "creating":
            assert self._create_model is not None
            self._create_model.serialized_parameters = serialized
        else:
            if self._update_model is None:
                self._update_model = self._update_model_cls()
            self._update_model.serialized_parameters = serialized
            self._dirty_fields.add("serialized_parameters")
            self._state = "dirty"

    def result_nowait(self) -> Any:
        """
        Unpack and return the Job result, or re-raise the exception.
        Raises `Job.NoResult` if there is no return value or exception yet.
        """
        if self._read_model is None:
            raise ValueError("Job data not yet loaded from API")
        s_ret = self._read_model.serialized_return_value
        s_exc = self._read_model.serialized_exception
        if s_ret:
            return deserialize(s_ret)
        elif s_exc:
            raise_from_serialized(s_exc)
        else:
            raise JobBase.NoResult("No return value or exception has been reported")

    def result(self, timeout: Optional[float] = None) -> Any:
        """
        Unpack and return the Job result, or re-raise the exception.
        Blocks for `timeout` sec and raises `concurrent.futures.TimeoutError` if there is no return value or exception yet.
        """
        wait_for: List["Job"] = [self]  # type: ignore
        res = self.objects.wait(wait_for, timeout=timeout)
        if res.done:
            return self.result_nowait()
        else:
            raise concurrent.futures.TimeoutError(f"Job is still {self.state} after {timeout} sec timeout")

    def done(self) -> bool:
        """
        Refresh the Job and return True if it has completed processing (either "JOB_FINISHED" or "FAILED" state.)
        """
        self.refresh_from_db()
        return self.state in [JobState.job_finished, JobState.failed]

    def resolve_workdir(self, data_path: Path) -> Path:
        return data_path.joinpath(self.workdir)

    def parent_query(self) -> "JobQuery":
        """
        Returns a JobQuery for the parents of this Job.
        """
        return self.objects.filter(id=list(self.parent_ids))


class JobWaitResult(NamedTuple):
    done: List["Job"]
    not_done: List["Job"]


class JobManagerBase(Manager["Job"]):
    _api_path = "jobs/"
    _bulk_create_enabled = True
    _bulk_update_enabled = True
    _bulk_delete_enabled = True

    def bulk_refresh(self, jobs: List["Job"]) -> None:
        """
        Refresh the list of Jobs from the latest database state
        """
        job_manager: "JobManager" = self  # type: ignore
        jobs_by_id = {job.id: job for job in jobs if job.id is not None}
        fetched = job_manager.filter(id=list(jobs_by_id.keys()))
        for api_job in fetched:
            assert api_job.id is not None and api_job._read_model is not None
            jobs_by_id[api_job.id]._refresh_from_dict(api_job._read_model.dict())

    def wait(
        self,
        jobs: List["Job"],
        timeout: Optional[float] = None,
        poll_interval: float = 1.0,
        return_when: str = "ALL_COMPLETED",
    ) -> JobWaitResult:
        """
        Block and periodically refresh jobs, until either all have completed
        (the default) or any have completed (return_when="FIRST_COMPLETED").
        Also returns after `timeout` seconds.  Rather than raising a Timeout
        error, returns a named tuple containing `done` and `not_done` Job lists.
        """

        start = time.time()
        done_jobs: List["Job"] = []
        not_done_jobs: List["Job"] = []

        for job in jobs:
            if job.state in DONE_STATES:
                done_jobs.append(job)
            else:
                not_done_jobs.append(job)

        def should_exit() -> bool:
            timed_out = (time.time() - start > timeout) if timeout is not None else False
            if return_when == "ALL_COMPLETED":
                return timed_out or len(not_done_jobs) == 0
            else:
                return timed_out or len(done_jobs) > 0

        while not should_exit():
            time.sleep(poll_interval)
            self.bulk_refresh(not_done_jobs)
            finished = [job for job in not_done_jobs if job.state in DONE_STATES]
            not_done_jobs[:] = [job for job in not_done_jobs if job.state not in DONE_STATES]
            done_jobs.extend(finished)

        return JobWaitResult(done=done_jobs, not_done=not_done_jobs)

    def as_completed(
        self,
        jobs: List["Job"],
        timeout: Optional[float] = None,
        poll_interval: float = 1.0,
    ) -> Iterator["Job"]:
        """
        Returns an iterator over the Job instances as they complete.  Raises a
        `concurrent.futures.TimeoutError` if __next__() is called and the result
        isn’t available after timeout seconds from the original call to
        as_completed().
        """

        start = time.time()
        pending_jobs: List["Job"] = []

        for job in jobs:
            if job.state in DONE_STATES:
                yield job
            else:
                pending_jobs.append(job)

        def should_exit() -> bool:
            timed_out = (time.time() - start > timeout) if timeout is not None else False
            return timed_out or not pending_jobs

        while not should_exit():
            time.sleep(poll_interval)
            self.bulk_refresh(pending_jobs)
            for job in pending_jobs:
                if job.state in DONE_STATES:
                    yield job
            pending_jobs[:] = [job for job in pending_jobs if job.state not in DONE_STATES]

        if pending_jobs:
            raise concurrent.futures.TimeoutError(
                f"{len(pending_jobs)} Jobs are still pending after {timeout} sec timeout."
            )


class SessionBase(CreatableBalsamModel):
    _create_model_cls = schemas.SessionCreate
    _update_model_cls = None
    _read_model_cls = schemas.SessionOut
    objects: "SessionManagerBase"

    def acquire_jobs(
        self,
        max_num_jobs: int,
        max_wall_time_min: Optional[int] = None,
        max_nodes_per_job: Optional[int] = None,
        max_aggregate_nodes: Optional[float] = None,
        serial_only: bool = False,
        sort_by: Optional[str] = None,
        filter_tags: Optional[Dict[str, str]] = None,
        states: Set[JobState] = RUNNABLE_STATES,
        app_ids: Optional[Set[int]] = None,
    ) -> "List[Job]":
        if filter_tags is None:
            filter_tags = {}
        if app_ids is None:
            app_ids = set()
        return self.__class__.objects._do_acquire(
            self,
            max_num_jobs=max_num_jobs,
            max_wall_time_min=max_wall_time_min,
            max_nodes_per_job=max_nodes_per_job,
            max_aggregate_nodes=max_aggregate_nodes,
            serial_only=serial_only,
            sort_by=sort_by,
            filter_tags=filter_tags,
            states=states,
            app_ids=app_ids,
        )

    def tick(self) -> None:
        return self.__class__.objects._do_tick(self)


class SessionManagerBase(Manager["Session"]):
    _api_path = "sessions/"
    _client: "RESTClient"

    def _do_acquire(self, instance: "SessionBase", **kwargs: Any) -> "List[Job]":
        from balsam._api.models import Job, JobManager

        Job.objects = JobManager(client=self._client)

        acquired_raw = self._client.post(self._api_path + f"{instance.id}", **kwargs)
        jobs = [Job._from_api(dat) for dat in acquired_raw]
        return jobs

    def _do_tick(self, instance: "SessionBase") -> None:
        self._client.put(self._api_path + f"{instance.id}")


class TransferItemBase(NonCreatableBalsamModel):
    _create_model_cls = None
    _update_model_cls = schemas.TransferItemUpdate
    _read_model_cls = schemas.TransferItemOut


class TransferItemManagerBase(Manager["TransferItem"]):
    _api_path = "transfers/"
    _bulk_update_enabled = True


class EventLogBase(NonCreatableBalsamModel):
    _create_model_cls = None
    _update_model_cls = None
    _read_model_cls = schemas.LogEventOut


class EventLogManagerBase(Manager["EventLog"]):
    _api_path = "events/"
