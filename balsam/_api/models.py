# This file was auto-generated via /Users/misha/workflow/balsam/env/bin/python balsam/schemas/api_generator.py
# [git rev e1e1657]
# Do *not* make changes to the API by changing this file!

import datetime
import pathlib
import typing
import uuid
from typing import Any, List, Optional, Union

import pydantic

import balsam._api.bases
import balsam._api.model
import balsam.server.routers.filters
from balsam._api.model import Field
from balsam._api.query import Query


class Site(balsam._api.bases.SiteBase):
    _create_model_cls = balsam.schemas.site.SiteCreate
    _update_model_cls = balsam.schemas.site.SiteUpdate
    _read_model_cls = balsam.schemas.site.SiteOut
    objects: "SiteManager"

    hostname = Field[str]()
    path = Field[pathlib.Path]()
    globus_endpoint_id = Field[Optional[uuid.UUID]]()
    num_nodes = Field[int]()
    backfill_windows = Field[typing.List[balsam.schemas.site.BackfillWindow]]()
    queued_jobs = Field[typing.List[balsam.schemas.site.QueuedJob]]()
    optional_batch_job_params = Field[typing.Dict[str, str]]()
    allowed_projects = Field[typing.List[str]]()
    allowed_queues = Field[typing.Dict[str, balsam.schemas.site.AllowedQueue]]()
    transfer_locations = Field[typing.Dict[str, pydantic.networks.AnyUrl]]()
    id = Field[Optional[int]]()
    last_refresh = Field[Optional[datetime.datetime]]()
    creation_date = Field[Optional[datetime.datetime]]()

    def __init__(
        self,
        hostname: str,
        path: pathlib.Path,
        globus_endpoint_id: Optional[uuid.UUID] = None,
        num_nodes: int = 0,
        backfill_windows: Optional[typing.List[balsam.schemas.site.BackfillWindow]] = None,
        queued_jobs: Optional[typing.List[balsam.schemas.site.QueuedJob]] = None,
        optional_batch_job_params: Optional[typing.Dict[str, str]] = None,
        allowed_projects: Optional[typing.List[str]] = None,
        allowed_queues: Optional[typing.Dict[str, balsam.schemas.site.AllowedQueue]] = None,
        transfer_locations: Optional[typing.Dict[str, pydantic.networks.AnyUrl]] = None,
        **kwargs: Any,
    ) -> None:
        _kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        _kwargs.update(kwargs)
        return super().__init__(**_kwargs)


class SiteQuery(Query[Site]):
    def get(
        self,
        hostname: Optional[str] = None,
        path: Optional[str] = None,
        id: Union[typing.List[int], int, None] = None,
    ) -> Site:
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._get(**kwargs)

    def filter(
        self,
        hostname: Optional[str] = None,
        path: Optional[str] = None,
        id: Union[typing.List[int], int, None] = None,
    ) -> "SiteQuery":
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._filter(**kwargs)

    def update(
        self,
        hostname: Optional[str] = None,
        path: Optional[pathlib.Path] = None,
        globus_endpoint_id: Optional[uuid.UUID] = None,
        num_nodes: Optional[int] = None,
        backfill_windows: Optional[typing.List[balsam.schemas.site.BackfillWindow]] = None,
        queued_jobs: Optional[typing.List[balsam.schemas.site.QueuedJob]] = None,
        optional_batch_job_params: Optional[typing.Dict[str, str]] = None,
        allowed_projects: Optional[typing.List[str]] = None,
        allowed_queues: Optional[typing.Dict[str, balsam.schemas.site.AllowedQueue]] = None,
        transfer_locations: Optional[typing.Dict[str, pydantic.networks.AnyUrl]] = None,
    ) -> List[Site]:
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._update(**kwargs)


class SiteManager(balsam._api.bases.SiteManagerBase):
    _api_path = "sites/"
    _model_class = Site
    _query_class = SiteQuery
    _bulk_create_enabled = False
    _bulk_update_enabled = False
    _bulk_delete_enabled = False
    _paginated_list_response = True

    def create(
        self,
        hostname: str,
        path: pathlib.Path,
        globus_endpoint_id: Optional[uuid.UUID] = None,
        num_nodes: int = 0,
        backfill_windows: Optional[typing.List[balsam.schemas.site.BackfillWindow]] = None,
        queued_jobs: Optional[typing.List[balsam.schemas.site.QueuedJob]] = None,
        optional_batch_job_params: Optional[typing.Dict[str, str]] = None,
        allowed_projects: Optional[typing.List[str]] = None,
        allowed_queues: Optional[typing.Dict[str, balsam.schemas.site.AllowedQueue]] = None,
        transfer_locations: Optional[typing.Dict[str, pydantic.networks.AnyUrl]] = None,
    ) -> Site:
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return super()._create(**kwargs)

    def all(self) -> "SiteQuery":
        return self._query_class(manager=self)

    def get(
        self,
        hostname: Optional[str] = None,
        path: Optional[str] = None,
        id: Union[typing.List[int], int, None] = None,
    ) -> Site:
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return SiteQuery(manager=self).get(**kwargs)

    def filter(
        self,
        hostname: Optional[str] = None,
        path: Optional[str] = None,
        id: Union[typing.List[int], int, None] = None,
    ) -> "SiteQuery":
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return SiteQuery(manager=self).filter(**kwargs)


class App(balsam._api.bases.AppBase):
    _create_model_cls = balsam.schemas.apps.AppCreate
    _update_model_cls = balsam.schemas.apps.AppUpdate
    _read_model_cls = balsam.schemas.apps.AppOut
    objects: "AppManager"

    site_id = Field[int]()
    description = Field[str]()
    class_path = Field[str]()
    parameters = Field[typing.Dict[str, balsam.schemas.apps.AppParameter]]()
    transfers = Field[typing.Dict[str, balsam.schemas.apps.TransferSlot]]()
    last_modified = Field[Optional[float]]()
    id = Field[Optional[int]]()

    def __init__(
        self,
        site_id: int,
        class_path: str,
        description: str = "",
        parameters: Optional[typing.Dict[str, balsam.schemas.apps.AppParameter]] = None,
        transfers: Optional[typing.Dict[str, balsam.schemas.apps.TransferSlot]] = None,
        last_modified: Optional[float] = None,
        **kwargs: Any,
    ) -> None:
        _kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        _kwargs.update(kwargs)
        return super().__init__(**_kwargs)


class AppQuery(Query[App]):
    def get(
        self,
        site_id: Union[typing.List[int], int, None] = None,
        id: Union[typing.List[int], int, None] = None,
        class_path: Optional[str] = None,
    ) -> App:
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._get(**kwargs)

    def filter(
        self,
        site_id: Union[typing.List[int], int, None] = None,
        id: Union[typing.List[int], int, None] = None,
        class_path: Optional[str] = None,
    ) -> "AppQuery":
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._filter(**kwargs)

    def update(
        self,
        site_id: Optional[int] = None,
        description: Optional[str] = None,
        class_path: Optional[str] = None,
        parameters: Optional[typing.Dict[str, balsam.schemas.apps.AppParameter]] = None,
        transfers: Optional[typing.Dict[str, balsam.schemas.apps.TransferSlot]] = None,
        last_modified: Optional[float] = None,
    ) -> List[App]:
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._update(**kwargs)


class AppManager(balsam._api.bases.AppManagerBase):
    _api_path = "apps/"
    _model_class = App
    _query_class = AppQuery
    _bulk_create_enabled = False
    _bulk_update_enabled = False
    _bulk_delete_enabled = False
    _paginated_list_response = True

    def create(
        self,
        site_id: int,
        class_path: str,
        description: str = "",
        parameters: Optional[typing.Dict[str, balsam.schemas.apps.AppParameter]] = None,
        transfers: Optional[typing.Dict[str, balsam.schemas.apps.TransferSlot]] = None,
        last_modified: Optional[float] = None,
    ) -> App:
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return super()._create(**kwargs)

    def all(self) -> "AppQuery":
        return self._query_class(manager=self)

    def get(
        self,
        site_id: Union[typing.List[int], int, None] = None,
        id: Union[typing.List[int], int, None] = None,
        class_path: Optional[str] = None,
    ) -> App:
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return AppQuery(manager=self).get(**kwargs)

    def filter(
        self,
        site_id: Union[typing.List[int], int, None] = None,
        id: Union[typing.List[int], int, None] = None,
        class_path: Optional[str] = None,
    ) -> "AppQuery":
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return AppQuery(manager=self).filter(**kwargs)


class Job(balsam._api.bases.JobBase):
    _create_model_cls = balsam.schemas.job.JobCreate
    _update_model_cls = balsam.schemas.job.JobUpdate
    _read_model_cls = balsam.schemas.job.JobOut
    objects: "JobManager"

    workdir = Field[pathlib.Path]()
    tags = Field[typing.Dict[str, str]]()
    parameters = Field[typing.Dict[str, str]]()
    data = Field[typing.Dict[str, typing.Any]]()
    return_code = Field[Optional[int]]()
    num_nodes = Field[int]()
    ranks_per_node = Field[int]()
    threads_per_rank = Field[int]()
    threads_per_core = Field[int]()
    launch_params = Field[typing.Dict[str, str]]()
    gpus_per_rank = Field[float]()
    node_packing_count = Field[int]()
    wall_time_min = Field[int]()
    app_id = Field[int]()
    parent_ids = Field[typing.Set[int]]()
    transfers = Field[typing.Dict[str, balsam.schemas.job.JobTransferItem]]()
    batch_job_id = Field[Optional[int]]()
    state = Field[Optional[balsam.schemas.job.JobState]]()
    state_timestamp = Field[Optional[datetime.datetime]]()
    state_data = Field[Optional[typing.Dict[str, typing.Any]]]()
    id = Field[Optional[int]]()
    last_update = Field[Optional[datetime.datetime]]()

    def __init__(
        self,
        workdir: pathlib.Path,
        app_id: int,
        tags: Optional[typing.Dict[str, str]] = None,
        parameters: Optional[typing.Dict[str, str]] = None,
        data: Optional[typing.Dict[str, typing.Any]] = None,
        return_code: Optional[int] = None,
        num_nodes: int = 1,
        ranks_per_node: int = 1,
        threads_per_rank: int = 1,
        threads_per_core: int = 1,
        launch_params: Optional[typing.Dict[str, str]] = None,
        gpus_per_rank: float = 0,
        node_packing_count: int = 1,
        wall_time_min: int = 0,
        parent_ids: typing.Set[int] = set(),
        transfers: Optional[typing.Dict[str, balsam.schemas.job.JobTransferItem]] = None,
        **kwargs: Any,
    ) -> None:
        _kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        _kwargs.update(kwargs)
        return super().__init__(**_kwargs)


class JobQuery(Query[Job]):
    def get(
        self,
        id: Union[typing.List[int], int, None] = None,
        parent_id: Union[typing.List[int], int, None] = None,
        app_id: Optional[int] = None,
        site_id: Optional[int] = None,
        batch_job_id: Optional[int] = None,
        last_update_before: Optional[datetime.datetime] = None,
        last_update_after: Optional[datetime.datetime] = None,
        workdir__contains: Optional[str] = None,
        state__ne: Optional[str] = None,
        state: Union[typing.List[str], str, None] = None,
        tags: Union[typing.List[str], str, None] = None,
        parameters: Union[typing.List[str], str, None] = None,
    ) -> Job:
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._get(**kwargs)

    def filter(
        self,
        id: Union[typing.List[int], int, None] = None,
        parent_id: Union[typing.List[int], int, None] = None,
        app_id: Optional[int] = None,
        site_id: Optional[int] = None,
        batch_job_id: Optional[int] = None,
        last_update_before: Optional[datetime.datetime] = None,
        last_update_after: Optional[datetime.datetime] = None,
        workdir__contains: Optional[str] = None,
        state__ne: Optional[str] = None,
        state: Union[typing.List[str], str, None] = None,
        tags: Union[typing.List[str], str, None] = None,
        parameters: Union[typing.List[str], str, None] = None,
    ) -> "JobQuery":
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._filter(**kwargs)

    def update(
        self,
        workdir: Optional[pathlib.Path] = None,
        tags: Optional[typing.Dict[str, str]] = None,
        parameters: Optional[typing.Dict[str, str]] = None,
        data: Optional[typing.Dict[str, typing.Any]] = None,
        return_code: Optional[int] = None,
        num_nodes: Optional[int] = None,
        ranks_per_node: Optional[int] = None,
        threads_per_rank: Optional[int] = None,
        threads_per_core: Optional[int] = None,
        launch_params: Optional[typing.Dict[str, str]] = None,
        gpus_per_rank: Optional[float] = None,
        node_packing_count: Optional[int] = None,
        wall_time_min: Optional[int] = None,
        batch_job_id: Optional[int] = None,
        state: Optional[balsam.schemas.job.JobState] = None,
        state_timestamp: Optional[datetime.datetime] = None,
        state_data: Optional[typing.Dict[str, typing.Any]] = None,
    ) -> List[Job]:
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._update(**kwargs)

    def order_by(self, field: Optional[balsam.server.routers.filters.JobOrdering]) -> "JobQuery":
        return self._order_by(field)


class JobManager(balsam._api.bases.JobManagerBase):
    _api_path = "jobs/"
    _model_class = Job
    _query_class = JobQuery
    _bulk_create_enabled = True
    _bulk_update_enabled = True
    _bulk_delete_enabled = True
    _paginated_list_response = True

    def create(
        self,
        workdir: pathlib.Path,
        app_id: int,
        tags: Optional[typing.Dict[str, str]] = None,
        parameters: Optional[typing.Dict[str, str]] = None,
        data: Optional[typing.Dict[str, typing.Any]] = None,
        return_code: Optional[int] = None,
        num_nodes: int = 1,
        ranks_per_node: int = 1,
        threads_per_rank: int = 1,
        threads_per_core: int = 1,
        launch_params: Optional[typing.Dict[str, str]] = None,
        gpus_per_rank: float = 0,
        node_packing_count: int = 1,
        wall_time_min: int = 0,
        parent_ids: typing.Set[int] = set(),
        transfers: Optional[typing.Dict[str, balsam.schemas.job.JobTransferItem]] = None,
    ) -> Job:
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return super()._create(**kwargs)

    def all(self) -> "JobQuery":
        return self._query_class(manager=self)

    def get(
        self,
        id: Union[typing.List[int], int, None] = None,
        parent_id: Union[typing.List[int], int, None] = None,
        app_id: Optional[int] = None,
        site_id: Optional[int] = None,
        batch_job_id: Optional[int] = None,
        last_update_before: Optional[datetime.datetime] = None,
        last_update_after: Optional[datetime.datetime] = None,
        workdir__contains: Optional[str] = None,
        state__ne: Optional[str] = None,
        state: Union[typing.List[str], str, None] = None,
        tags: Union[typing.List[str], str, None] = None,
        parameters: Union[typing.List[str], str, None] = None,
    ) -> Job:
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return JobQuery(manager=self).get(**kwargs)

    def filter(
        self,
        id: Union[typing.List[int], int, None] = None,
        parent_id: Union[typing.List[int], int, None] = None,
        app_id: Optional[int] = None,
        site_id: Optional[int] = None,
        batch_job_id: Optional[int] = None,
        last_update_before: Optional[datetime.datetime] = None,
        last_update_after: Optional[datetime.datetime] = None,
        workdir__contains: Optional[str] = None,
        state__ne: Optional[str] = None,
        state: Union[typing.List[str], str, None] = None,
        tags: Union[typing.List[str], str, None] = None,
        parameters: Union[typing.List[str], str, None] = None,
    ) -> "JobQuery":
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return JobQuery(manager=self).filter(**kwargs)


class BatchJob(balsam._api.bases.BatchJobBase):
    _create_model_cls = balsam.schemas.batchjob.BatchJobCreate
    _update_model_cls = balsam.schemas.batchjob.BatchJobUpdate
    _read_model_cls = balsam.schemas.batchjob.BatchJobOut
    objects: "BatchJobManager"

    num_nodes = Field[int]()
    wall_time_min = Field[int]()
    job_mode = Field[balsam.schemas.batchjob.JobMode]()
    optional_params = Field[typing.Dict[str, str]]()
    filter_tags = Field[typing.Dict[str, str]]()
    partitions = Field[typing.Optional[typing.List[balsam.schemas.batchjob.BatchJobPartition]]]()
    site_id = Field[int]()
    project = Field[str]()
    queue = Field[str]()
    scheduler_id = Field[Optional[int]]()
    state = Field[Optional[balsam.schemas.batchjob.BatchJobState]]()
    status_info = Field[Optional[typing.Dict[str, str]]]()
    start_time = Field[Optional[datetime.datetime]]()
    end_time = Field[Optional[datetime.datetime]]()
    id = Field[Optional[int]]()

    def __init__(
        self,
        num_nodes: int,
        wall_time_min: int,
        job_mode: balsam.schemas.batchjob.JobMode,
        site_id: int,
        project: str,
        queue: str,
        optional_params: Optional[typing.Dict[str, str]] = None,
        filter_tags: Optional[typing.Dict[str, str]] = None,
        partitions: Optional[typing.Optional[typing.List[balsam.schemas.batchjob.BatchJobPartition]]] = None,
        **kwargs: Any,
    ) -> None:
        _kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        _kwargs.update(kwargs)
        return super().__init__(**_kwargs)


class BatchJobQuery(Query[BatchJob]):
    def get(
        self,
        site_id: Union[typing.List[int], int, None] = None,
        state: Union[typing.List[str], str, None] = None,
        scheduler_id: Optional[int] = None,
        queue: Optional[str] = None,
        start_time_before: Optional[datetime.datetime] = None,
        start_time_after: Optional[datetime.datetime] = None,
        end_time_before: Optional[datetime.datetime] = None,
        end_time_after: Optional[datetime.datetime] = None,
        filter_tags: Union[typing.List[str], str, None] = None,
    ) -> BatchJob:
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._get(**kwargs)

    def filter(
        self,
        site_id: Union[typing.List[int], int, None] = None,
        state: Union[typing.List[str], str, None] = None,
        scheduler_id: Optional[int] = None,
        queue: Optional[str] = None,
        start_time_before: Optional[datetime.datetime] = None,
        start_time_after: Optional[datetime.datetime] = None,
        end_time_before: Optional[datetime.datetime] = None,
        end_time_after: Optional[datetime.datetime] = None,
        filter_tags: Union[typing.List[str], str, None] = None,
    ) -> "BatchJobQuery":
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._filter(**kwargs)

    def update(
        self,
        scheduler_id: Optional[int] = None,
        state: Optional[balsam.schemas.batchjob.BatchJobState] = None,
        status_info: Optional[typing.Dict[str, str]] = None,
        start_time: Optional[datetime.datetime] = None,
        end_time: Optional[datetime.datetime] = None,
    ) -> List[BatchJob]:
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._update(**kwargs)

    def order_by(self, field: Optional[balsam.server.routers.filters.BatchJobOrdering]) -> "BatchJobQuery":
        return self._order_by(field)


class BatchJobManager(balsam._api.bases.BatchJobManagerBase):
    _api_path = "batch-jobs/"
    _model_class = BatchJob
    _query_class = BatchJobQuery
    _bulk_create_enabled = False
    _bulk_update_enabled = True
    _bulk_delete_enabled = False
    _paginated_list_response = True

    def create(
        self,
        num_nodes: int,
        wall_time_min: int,
        job_mode: balsam.schemas.batchjob.JobMode,
        site_id: int,
        project: str,
        queue: str,
        optional_params: Optional[typing.Dict[str, str]] = None,
        filter_tags: Optional[typing.Dict[str, str]] = None,
        partitions: Optional[typing.Optional[typing.List[balsam.schemas.batchjob.BatchJobPartition]]] = None,
    ) -> BatchJob:
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return super()._create(**kwargs)

    def all(self) -> "BatchJobQuery":
        return self._query_class(manager=self)

    def get(
        self,
        site_id: Union[typing.List[int], int, None] = None,
        state: Union[typing.List[str], str, None] = None,
        scheduler_id: Optional[int] = None,
        queue: Optional[str] = None,
        start_time_before: Optional[datetime.datetime] = None,
        start_time_after: Optional[datetime.datetime] = None,
        end_time_before: Optional[datetime.datetime] = None,
        end_time_after: Optional[datetime.datetime] = None,
        filter_tags: Union[typing.List[str], str, None] = None,
    ) -> BatchJob:
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return BatchJobQuery(manager=self).get(**kwargs)

    def filter(
        self,
        site_id: Union[typing.List[int], int, None] = None,
        state: Union[typing.List[str], str, None] = None,
        scheduler_id: Optional[int] = None,
        queue: Optional[str] = None,
        start_time_before: Optional[datetime.datetime] = None,
        start_time_after: Optional[datetime.datetime] = None,
        end_time_before: Optional[datetime.datetime] = None,
        end_time_after: Optional[datetime.datetime] = None,
        filter_tags: Union[typing.List[str], str, None] = None,
    ) -> "BatchJobQuery":
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return BatchJobQuery(manager=self).filter(**kwargs)


class Session(balsam._api.bases.SessionBase):
    _create_model_cls = balsam.schemas.session.SessionCreate
    _update_model_cls = None
    _read_model_cls = balsam.schemas.session.SessionOut
    objects: "SessionManager"

    site_id = Field[Optional[int]]()
    batch_job_id = Field[Optional[int]]()
    id = Field[Optional[int]]()
    heartbeat = Field[Optional[datetime.datetime]]()

    def __init__(
        self,
        site_id: int,
        batch_job_id: Optional[int] = None,
        **kwargs: Any,
    ) -> None:
        _kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        _kwargs.update(kwargs)
        return super().__init__(**_kwargs)


class SessionQuery(Query[Session]):
    pass


class SessionManager(balsam._api.bases.SessionManagerBase):
    _api_path = "sessions/"
    _model_class = Session
    _query_class = SessionQuery
    _bulk_create_enabled = False
    _bulk_update_enabled = False
    _bulk_delete_enabled = False
    _paginated_list_response = True

    def create(
        self,
        site_id: int,
        batch_job_id: Optional[int] = None,
    ) -> Session:
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return super()._create(**kwargs)

    def all(self) -> "SessionQuery":
        return self._query_class(manager=self)


class TransferItem(balsam._api.bases.TransferItemBase):
    _create_model_cls = None
    _update_model_cls = balsam.schemas.transfer.TransferItemUpdate
    _read_model_cls = balsam.schemas.transfer.TransferItemOut
    objects: "TransferItemManager"

    state = Field[Optional[balsam.schemas.transfer.TransferItemState]]()
    task_id = Field[Optional[str]]()
    transfer_info = Field[Optional[typing.Dict[str, typing.Any]]]()
    id = Field[Optional[int]]()
    job_id = Field[Optional[int]]()
    direction = Field[Optional[balsam.schemas.transfer.TransferDirection]]()
    local_path = Field[Optional[pathlib.Path]]()
    remote_path = Field[Optional[pathlib.Path]]()
    location_alias = Field[Optional[str]]()
    recursive = Field[Optional[bool]]()


class TransferItemQuery(Query[TransferItem]):
    def get(
        self,
        id: Union[typing.List[int], int, None] = None,
        site_id: Optional[int] = None,
        job_id: Union[typing.List[int], int, None] = None,
        state: Union[
            typing.Set[balsam.schemas.transfer.TransferItemState], balsam.schemas.transfer.TransferItemState, None
        ] = None,
        direction: Optional[balsam.schemas.transfer.TransferDirection] = None,
        job_state: Optional[str] = None,
        tags: Union[typing.List[str], str, None] = None,
    ) -> TransferItem:
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._get(**kwargs)

    def filter(
        self,
        id: Union[typing.List[int], int, None] = None,
        site_id: Optional[int] = None,
        job_id: Union[typing.List[int], int, None] = None,
        state: Union[
            typing.Set[balsam.schemas.transfer.TransferItemState], balsam.schemas.transfer.TransferItemState, None
        ] = None,
        direction: Optional[balsam.schemas.transfer.TransferDirection] = None,
        job_state: Optional[str] = None,
        tags: Union[typing.List[str], str, None] = None,
    ) -> "TransferItemQuery":
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._filter(**kwargs)

    def update(
        self,
        state: Optional[balsam.schemas.transfer.TransferItemState] = None,
        task_id: Optional[str] = None,
        transfer_info: Optional[typing.Dict[str, typing.Any]] = None,
    ) -> List[TransferItem]:
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._update(**kwargs)


class TransferItemManager(balsam._api.bases.TransferItemManagerBase):
    _api_path = "transfers/"
    _model_class = TransferItem
    _query_class = TransferItemQuery
    _bulk_create_enabled = False
    _bulk_update_enabled = True
    _bulk_delete_enabled = False
    _paginated_list_response = True

    def all(self) -> "TransferItemQuery":
        return self._query_class(manager=self)

    def get(
        self,
        id: Union[typing.List[int], int, None] = None,
        site_id: Optional[int] = None,
        job_id: Union[typing.List[int], int, None] = None,
        state: Union[
            typing.Set[balsam.schemas.transfer.TransferItemState], balsam.schemas.transfer.TransferItemState, None
        ] = None,
        direction: Optional[balsam.schemas.transfer.TransferDirection] = None,
        job_state: Optional[str] = None,
        tags: Union[typing.List[str], str, None] = None,
    ) -> TransferItem:
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return TransferItemQuery(manager=self).get(**kwargs)

    def filter(
        self,
        id: Union[typing.List[int], int, None] = None,
        site_id: Optional[int] = None,
        job_id: Union[typing.List[int], int, None] = None,
        state: Union[
            typing.Set[balsam.schemas.transfer.TransferItemState], balsam.schemas.transfer.TransferItemState, None
        ] = None,
        direction: Optional[balsam.schemas.transfer.TransferDirection] = None,
        job_state: Optional[str] = None,
        tags: Union[typing.List[str], str, None] = None,
    ) -> "TransferItemQuery":
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return TransferItemQuery(manager=self).filter(**kwargs)


class EventLog(balsam._api.bases.EventLogBase):
    _create_model_cls = None
    _update_model_cls = None
    _read_model_cls = balsam.schemas.logevent.LogEventOut
    objects: "EventLogManager"

    id = Field[Optional[int]]()
    job_id = Field[Optional[int]]()
    timestamp = Field[Optional[datetime.datetime]]()
    from_state = Field[Optional[str]]()
    to_state = Field[Optional[str]]()
    data = Field[Optional[typing.Dict[str, typing.Any]]]()


class EventLogQuery(Query[EventLog]):
    def get(
        self,
        job_id: Union[typing.List[int], int, None] = None,
        batch_job_id: Optional[int] = None,
        scheduler_id: Optional[int] = None,
        tags: Union[typing.List[str], str, None] = None,
        data: Union[typing.List[str], str, None] = None,
        timestamp_before: Optional[datetime.datetime] = None,
        timestamp_after: Optional[datetime.datetime] = None,
        from_state: Optional[str] = None,
        to_state: Optional[str] = None,
    ) -> EventLog:
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._get(**kwargs)

    def filter(
        self,
        job_id: Union[typing.List[int], int, None] = None,
        batch_job_id: Optional[int] = None,
        scheduler_id: Optional[int] = None,
        tags: Union[typing.List[str], str, None] = None,
        data: Union[typing.List[str], str, None] = None,
        timestamp_before: Optional[datetime.datetime] = None,
        timestamp_after: Optional[datetime.datetime] = None,
        from_state: Optional[str] = None,
        to_state: Optional[str] = None,
    ) -> "EventLogQuery":
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._filter(**kwargs)

    def order_by(self, field: Optional[balsam.server.routers.filters.EventOrdering]) -> "EventLogQuery":
        return self._order_by(field)


class EventLogManager(balsam._api.bases.EventLogManagerBase):
    _api_path = "events/"
    _model_class = EventLog
    _query_class = EventLogQuery
    _bulk_create_enabled = False
    _bulk_update_enabled = False
    _bulk_delete_enabled = False
    _paginated_list_response = True

    def all(self) -> "EventLogQuery":
        return self._query_class(manager=self)

    def get(
        self,
        job_id: Union[typing.List[int], int, None] = None,
        batch_job_id: Optional[int] = None,
        scheduler_id: Optional[int] = None,
        tags: Union[typing.List[str], str, None] = None,
        data: Union[typing.List[str], str, None] = None,
        timestamp_before: Optional[datetime.datetime] = None,
        timestamp_after: Optional[datetime.datetime] = None,
        from_state: Optional[str] = None,
        to_state: Optional[str] = None,
    ) -> EventLog:
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return EventLogQuery(manager=self).get(**kwargs)

    def filter(
        self,
        job_id: Union[typing.List[int], int, None] = None,
        batch_job_id: Optional[int] = None,
        scheduler_id: Optional[int] = None,
        tags: Union[typing.List[str], str, None] = None,
        data: Union[typing.List[str], str, None] = None,
        timestamp_before: Optional[datetime.datetime] = None,
        timestamp_after: Optional[datetime.datetime] = None,
        from_state: Optional[str] = None,
        to_state: Optional[str] = None,
    ) -> "EventLogQuery":
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return EventLogQuery(manager=self).filter(**kwargs)
