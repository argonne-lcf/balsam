# This file was auto-generated via /Users/misha/workflow/balsam/.venv/bin/python balsam/schemas/api_generator.py
# [git rev 9ec37c2]
# Do *not* make changes to the API by changing this file!

import datetime
import pathlib
import typing
import uuid
from typing import Any, List, Optional, Union

import pydantic

import balsam._api.bases
import balsam._api.model
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
    backfill_windows = Field[typing.Dict[str, typing.List[balsam.schemas.batchjob.SchedulerBackfillWindow]]]()
    queued_jobs = Field[typing.Dict[int, balsam.schemas.batchjob.SchedulerJobStatus]]()
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
        backfill_windows: Optional[
            typing.Dict[str, typing.List[balsam.schemas.batchjob.SchedulerBackfillWindow]]
        ] = None,
        queued_jobs: Optional[typing.Dict[int, balsam.schemas.batchjob.SchedulerJobStatus]] = None,
        optional_batch_job_params: Optional[typing.Dict[str, str]] = None,
        allowed_projects: Optional[typing.List[str]] = None,
        allowed_queues: Optional[typing.Dict[str, balsam.schemas.site.AllowedQueue]] = None,
        transfer_locations: Optional[typing.Dict[str, pydantic.networks.AnyUrl]] = None,
        **kwargs: Any,
    ) -> None:
        """
        Construct a new Site object.  You must eventually call the save() method or
        pass a Site list into Site.objects.bulk_create().

        hostname:                  The Site network location, for human reference only
        path:                      Absolute filesystem path of the Site
        globus_endpoint_id:        Associated Globus endpoint ID
        backfill_windows:          Idle backfill currently available at the Site, keyed by queue name
        queued_jobs:               Queued scheduler jobs at the Site, keyed by scheduler ID
        optional_batch_job_params: Optional pass-through parameters accepted by the Site batchjob template
        allowed_projects:          Allowed projects/allocations for batchjob submission
        allowed_queues:            Allowed queues and associated queueing policies
        transfer_locations:        Trusted transfer location aliases and associated protocol/URLs
        """
        _kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        _kwargs.update(kwargs)
        return super().__init__(**_kwargs)


class SiteQuery(Query[Site]):
    def get(
        self,
        hostname: Optional[str] = None,
        path: Optional[str] = None,
        id: Union[typing.List[int], int, None] = None,
        last_refresh_after: Optional[datetime.datetime] = None,
    ) -> Site:
        """
        Retrieve exactly one Site. Raises Site.DoesNotExist
        if no items were found, or Site.MultipleObjectsReturned if
        more than one item matched the query.

        hostname:           Only return Sites with hostnames containing this string.
        path:               Only return Sites with paths containing this string.
        id:                 Only return Sites having an id in this list.
        last_refresh_after: Only return Sites active since this time (UTC)
        """
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._get(**kwargs)

    def filter(
        self,
        hostname: Optional[str] = None,
        path: Optional[str] = None,
        id: Union[typing.List[int], int, None] = None,
        last_refresh_after: Optional[datetime.datetime] = None,
    ) -> "SiteQuery":
        """
        Retrieve exactly one Site. Raises Site.DoesNotExist
        if no items were found, or Site.MultipleObjectsReturned if
        more than one item matched the query.

        hostname:           Only return Sites with hostnames containing this string.
        path:               Only return Sites with paths containing this string.
        id:                 Only return Sites having an id in this list.
        last_refresh_after: Only return Sites active since this time (UTC)
        """
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._filter(**kwargs)

    def update(
        self,
        hostname: Optional[str] = None,
        path: Optional[pathlib.Path] = None,
        globus_endpoint_id: Optional[uuid.UUID] = None,
        backfill_windows: Optional[
            typing.Dict[str, typing.List[balsam.schemas.batchjob.SchedulerBackfillWindow]]
        ] = None,
        queued_jobs: Optional[typing.Dict[int, balsam.schemas.batchjob.SchedulerJobStatus]] = None,
        optional_batch_job_params: Optional[typing.Dict[str, str]] = None,
        allowed_projects: Optional[typing.List[str]] = None,
        allowed_queues: Optional[typing.Dict[str, balsam.schemas.site.AllowedQueue]] = None,
        transfer_locations: Optional[typing.Dict[str, pydantic.networks.AnyUrl]] = None,
    ) -> List[Site]:
        """
        Updates all items selected by this query with the given values.

        hostname:                  The Site network location, for human reference only
        path:                      Absolute filesystem path of the Site
        globus_endpoint_id:        Associated Globus endpoint ID
        backfill_windows:          Idle backfill currently available at the Site, keyed by queue name
        queued_jobs:               Queued scheduler jobs at the Site, keyed by scheduler ID
        optional_batch_job_params: Optional pass-through parameters accepted by the Site batchjob template
        allowed_projects:          Allowed projects/allocations for batchjob submission
        allowed_queues:            Allowed queues and associated queueing policies
        transfer_locations:        Trusted transfer location aliases and associated protocol/URLs
        """
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
        backfill_windows: Optional[
            typing.Dict[str, typing.List[balsam.schemas.batchjob.SchedulerBackfillWindow]]
        ] = None,
        queued_jobs: Optional[typing.Dict[int, balsam.schemas.batchjob.SchedulerJobStatus]] = None,
        optional_batch_job_params: Optional[typing.Dict[str, str]] = None,
        allowed_projects: Optional[typing.List[str]] = None,
        allowed_queues: Optional[typing.Dict[str, balsam.schemas.site.AllowedQueue]] = None,
        transfer_locations: Optional[typing.Dict[str, pydantic.networks.AnyUrl]] = None,
    ) -> Site:
        """
        Create a new Site object and save it to the API in one step.

        hostname:                  The Site network location, for human reference only
        path:                      Absolute filesystem path of the Site
        globus_endpoint_id:        Associated Globus endpoint ID
        backfill_windows:          Idle backfill currently available at the Site, keyed by queue name
        queued_jobs:               Queued scheduler jobs at the Site, keyed by scheduler ID
        optional_batch_job_params: Optional pass-through parameters accepted by the Site batchjob template
        allowed_projects:          Allowed projects/allocations for batchjob submission
        allowed_queues:            Allowed queues and associated queueing policies
        transfer_locations:        Trusted transfer location aliases and associated protocol/URLs
        """
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return super()._create(**kwargs)

    def all(self) -> "SiteQuery":
        """
        Returns a Query for all Site items.
        """
        return self._query_class(manager=self)

    def get(
        self,
        hostname: Optional[str] = None,
        path: Optional[str] = None,
        id: Union[typing.List[int], int, None] = None,
        last_refresh_after: Optional[datetime.datetime] = None,
    ) -> Site:
        """
        Retrieve exactly one Site. Raises Site.DoesNotExist
        if no items were found, or Site.MultipleObjectsReturned if
        more than one item matched the query.

        hostname:           Only return Sites with hostnames containing this string.
        path:               Only return Sites with paths containing this string.
        id:                 Only return Sites having an id in this list.
        last_refresh_after: Only return Sites active since this time (UTC)
        """
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return SiteQuery(manager=self).get(**kwargs)

    def filter(
        self,
        hostname: Optional[str] = None,
        path: Optional[str] = None,
        id: Union[typing.List[int], int, None] = None,
        last_refresh_after: Optional[datetime.datetime] = None,
    ) -> "SiteQuery":
        """
        Returns a Site Query returning items matching the filter criteria.

        hostname:           Only return Sites with hostnames containing this string.
        path:               Only return Sites with paths containing this string.
        id:                 Only return Sites having an id in this list.
        last_refresh_after: Only return Sites active since this time (UTC)
        """
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
        """
        Construct a new App object.  You must eventually call the save() method or
        pass a App list into App.objects.bulk_create().

        site_id:       Site id at which this App is registered
        description:   The App class docstring
        class_path:    Python class path (module.ClassName) of this App
        parameters:    Allowed parameters in the App command
        transfers:     Allowed transfer slots in the App
        last_modified: Local timestamp since App module file last changed
        """
        _kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        _kwargs.update(kwargs)
        return super().__init__(**_kwargs)


class AppQuery(Query[App]):
    def get(
        self,
        site_id: Union[typing.List[int], int, None] = None,
        id: Union[typing.List[int], int, None] = None,
        class_path: Optional[str] = None,
        site_path: Optional[str] = None,
    ) -> App:
        """
        Retrieve exactly one App. Raises App.DoesNotExist
        if no items were found, or App.MultipleObjectsReturned if
        more than one item matched the query.

        site_id:    Only return Apps associated with the Site IDs in this list.
        id:         Only return Apps with IDs in this list.
        class_path: Only return Apps matching this dotted class path (module.ClassName)
        site_path:  Only return Apps from Sites having paths containing this substring.
        """
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._get(**kwargs)

    def filter(
        self,
        site_id: Union[typing.List[int], int, None] = None,
        id: Union[typing.List[int], int, None] = None,
        class_path: Optional[str] = None,
        site_path: Optional[str] = None,
    ) -> "AppQuery":
        """
        Retrieve exactly one App. Raises App.DoesNotExist
        if no items were found, or App.MultipleObjectsReturned if
        more than one item matched the query.

        site_id:    Only return Apps associated with the Site IDs in this list.
        id:         Only return Apps with IDs in this list.
        class_path: Only return Apps matching this dotted class path (module.ClassName)
        site_path:  Only return Apps from Sites having paths containing this substring.
        """
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
        """
        Updates all items selected by this query with the given values.

        site_id:       Site id at which this App is registered
        description:   The App class docstring
        class_path:    Python class path (module.ClassName) of this App
        parameters:    Allowed parameters in the App command
        transfers:     Allowed transfer slots in the App
        last_modified: Local timestamp since App module file last changed
        """
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
        """
        Create a new App object and save it to the API in one step.

        site_id:       Site id at which this App is registered
        description:   The App class docstring
        class_path:    Python class path (module.ClassName) of this App
        parameters:    Allowed parameters in the App command
        transfers:     Allowed transfer slots in the App
        last_modified: Local timestamp since App module file last changed
        """
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return super()._create(**kwargs)

    def all(self) -> "AppQuery":
        """
        Returns a Query for all App items.
        """
        return self._query_class(manager=self)

    def get(
        self,
        site_id: Union[typing.List[int], int, None] = None,
        id: Union[typing.List[int], int, None] = None,
        class_path: Optional[str] = None,
        site_path: Optional[str] = None,
    ) -> App:
        """
        Retrieve exactly one App. Raises App.DoesNotExist
        if no items were found, or App.MultipleObjectsReturned if
        more than one item matched the query.

        site_id:    Only return Apps associated with the Site IDs in this list.
        id:         Only return Apps with IDs in this list.
        class_path: Only return Apps matching this dotted class path (module.ClassName)
        site_path:  Only return Apps from Sites having paths containing this substring.
        """
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return AppQuery(manager=self).get(**kwargs)

    def filter(
        self,
        site_id: Union[typing.List[int], int, None] = None,
        id: Union[typing.List[int], int, None] = None,
        class_path: Optional[str] = None,
        site_path: Optional[str] = None,
    ) -> "AppQuery":
        """
        Returns a App Query returning items matching the filter criteria.

        site_id:    Only return Apps associated with the Site IDs in this list.
        id:         Only return Apps with IDs in this list.
        class_path: Only return Apps matching this dotted class path (module.ClassName)
        site_path:  Only return Apps from Sites having paths containing this substring.
        """
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return AppQuery(manager=self).filter(**kwargs)


class Job(balsam._api.bases.JobBase):
    _create_model_cls = balsam.schemas.job.ClientJobCreate
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
    app_name = Field[str]()
    site_path = Field[str]()
    parent_ids = Field[typing.Set[int]]()
    transfers = Field[typing.Dict[str, typing.Union[str, balsam.schemas.job.JobTransferItem]]]()
    batch_job_id = Field[Optional[int]]()
    state = Field[Optional[balsam.schemas.job.JobState]]()
    state_timestamp = Field[Optional[datetime.datetime]]()
    state_data = Field[Optional[typing.Dict[str, typing.Any]]]()
    pending_file_cleanup = Field[Optional[bool]]()
    id = Field[Optional[int]]()
    last_update = Field[Optional[datetime.datetime]]()

    def __init__(
        self,
        workdir: pathlib.Path,
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
        app_id: Optional[int] = None,
        app_name: Optional[str] = None,
        site_path: Optional[str] = None,
        parent_ids: typing.Set[int] = set(),
        transfers: Optional[typing.Dict[str, typing.Union[str, balsam.schemas.job.JobTransferItem]]] = None,
        **kwargs: Any,
    ) -> None:
        """
        Construct a new Job object.  You must eventually call the save() method or
        pass a Job list into Job.objects.bulk_create().

        workdir:            Job path relative to site data/ folder.
        tags:               Custom key:value string tags.
        parameters:         App parameter name:value pairs.
        data:               Arbitrary JSON-able data dictionary.
        return_code:        Return code from last execution of this Job.
        num_nodes:          Number of compute nodes needed.
        ranks_per_node:     Number of MPI processes per node.
        threads_per_rank:   Logical threads per process.
        threads_per_core:   Logical threads per CPU core.
        launch_params:      Optional pass-through parameters to MPI application launcher.
        gpus_per_rank:      Number of GPUs per process.
        node_packing_count: Maximum number of concurrent runs per node.
        wall_time_min:      Optional estimate of Job runtime. All else being equal, longer Jobs tend to run first.
        app_id:             App ID
        app_name:           App Class Name
        site_path:          Site Path Substring
        parent_ids:         Set of parent Job IDs (dependencies).
        transfers:          TransferItem dictionary. One key:JobTransferItem pair for each slot defined on the App.
        """
        _kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        _kwargs.update(kwargs)
        return super().__init__(**_kwargs)


class JobQuery(Query[Job]):
    def get(
        self,
        id: Union[typing.List[int], int, None] = None,
        parent_id: Union[typing.List[int], int, None] = None,
        app_id: Optional[int] = None,
        site_id: Union[typing.List[int], int, None] = None,
        batch_job_id: Optional[int] = None,
        last_update_before: Optional[datetime.datetime] = None,
        last_update_after: Optional[datetime.datetime] = None,
        workdir__contains: Optional[str] = None,
        state__ne: Optional[balsam.schemas.job.JobState] = None,
        state: Union[typing.Set[balsam.schemas.job.JobState], balsam.schemas.job.JobState, None] = None,
        tags: Union[typing.List[str], str, None] = None,
        parameters: Union[typing.List[str], str, None] = None,
        pending_file_cleanup: Optional[bool] = None,
    ) -> Job:
        """
        Retrieve exactly one Job. Raises Job.DoesNotExist
        if no items were found, or Job.MultipleObjectsReturned if
        more than one item matched the query.

        id:                   Only return Jobs with ids in this list.
        parent_id:            Only return Jobs that are children of Jobs with ids in this list.
        app_id:               Only return Jobs associated with this App id.
        site_id:              Only return Jobs associated with these Site ids.
        batch_job_id:         Only return Jobs associated with this BatchJob id.
        last_update_before:   Only return Jobs that were updated before this time (UTC).
        last_update_after:    Only return Jobs that were updated after this time (UTC).
        workdir__contains:    Only return jobs with workdirs containing this string.
        state__ne:            Only return jobs with states not equal to this state.
        state:                Only return jobs in this set of states.
        tags:                 Only return jobs containing these tags (list of KEY:VALUE strings)
        parameters:           Only return jobs having these App command parameters (list of KEY:VALUE strings)
        pending_file_cleanup: Only return jobs which have not yet had workdir cleaned.
        """
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._get(**kwargs)

    def filter(
        self,
        id: Union[typing.List[int], int, None] = None,
        parent_id: Union[typing.List[int], int, None] = None,
        app_id: Optional[int] = None,
        site_id: Union[typing.List[int], int, None] = None,
        batch_job_id: Optional[int] = None,
        last_update_before: Optional[datetime.datetime] = None,
        last_update_after: Optional[datetime.datetime] = None,
        workdir__contains: Optional[str] = None,
        state__ne: Optional[balsam.schemas.job.JobState] = None,
        state: Union[typing.Set[balsam.schemas.job.JobState], balsam.schemas.job.JobState, None] = None,
        tags: Union[typing.List[str], str, None] = None,
        parameters: Union[typing.List[str], str, None] = None,
        pending_file_cleanup: Optional[bool] = None,
    ) -> "JobQuery":
        """
        Retrieve exactly one Job. Raises Job.DoesNotExist
        if no items were found, or Job.MultipleObjectsReturned if
        more than one item matched the query.

        id:                   Only return Jobs with ids in this list.
        parent_id:            Only return Jobs that are children of Jobs with ids in this list.
        app_id:               Only return Jobs associated with this App id.
        site_id:              Only return Jobs associated with these Site ids.
        batch_job_id:         Only return Jobs associated with this BatchJob id.
        last_update_before:   Only return Jobs that were updated before this time (UTC).
        last_update_after:    Only return Jobs that were updated after this time (UTC).
        workdir__contains:    Only return jobs with workdirs containing this string.
        state__ne:            Only return jobs with states not equal to this state.
        state:                Only return jobs in this set of states.
        tags:                 Only return jobs containing these tags (list of KEY:VALUE strings)
        parameters:           Only return jobs having these App command parameters (list of KEY:VALUE strings)
        pending_file_cleanup: Only return jobs which have not yet had workdir cleaned.
        """
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
        pending_file_cleanup: Optional[bool] = None,
    ) -> List[Job]:
        """
        Updates all items selected by this query with the given values.

        workdir:              Job path relative to the site data/ folder
        tags:                 Custom key:value string tags.
        parameters:           App parameter name:value pairs.
        data:                 Arbitrary JSON-able data dictionary.
        return_code:          Return code from last execution of this Job.
        num_nodes:            Number of compute nodes needed.
        ranks_per_node:       Number of MPI processes per node.
        threads_per_rank:     Logical threads per process.
        threads_per_core:     Logical threads per CPU core.
        launch_params:        Optional pass-through parameters to MPI application launcher.
        gpus_per_rank:        Number of GPUs per process.
        node_packing_count:   Maximum number of concurrent runs per node.
        wall_time_min:        Optional estimate of Job runtime. All else being equal, longer Jobs tend to run first.
        batch_job_id:         ID of most recent BatchJob in which this Job ran
        state:                Job state
        state_timestamp:      Time (UTC) at which Job state change occured
        state_data:           Arbitrary associated state change data for logging
        pending_file_cleanup: Whether job remains to have workdir cleaned.
        """
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._update(**kwargs)

    def order_by(self, field: Optional[balsam.schemas.job.JobOrdering]) -> "JobQuery":
        """
        Order the returned items by this field.
        """
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
        app_id: Optional[int] = None,
        app_name: Optional[str] = None,
        site_path: Optional[str] = None,
        parent_ids: typing.Set[int] = set(),
        transfers: Optional[typing.Dict[str, typing.Union[str, balsam.schemas.job.JobTransferItem]]] = None,
    ) -> Job:
        """
        Create a new Job object and save it to the API in one step.

        workdir:            Job path relative to site data/ folder.
        tags:               Custom key:value string tags.
        parameters:         App parameter name:value pairs.
        data:               Arbitrary JSON-able data dictionary.
        return_code:        Return code from last execution of this Job.
        num_nodes:          Number of compute nodes needed.
        ranks_per_node:     Number of MPI processes per node.
        threads_per_rank:   Logical threads per process.
        threads_per_core:   Logical threads per CPU core.
        launch_params:      Optional pass-through parameters to MPI application launcher.
        gpus_per_rank:      Number of GPUs per process.
        node_packing_count: Maximum number of concurrent runs per node.
        wall_time_min:      Optional estimate of Job runtime. All else being equal, longer Jobs tend to run first.
        app_id:             App ID
        app_name:           App Class Name
        site_path:          Site Path Substring
        parent_ids:         Set of parent Job IDs (dependencies).
        transfers:          TransferItem dictionary. One key:JobTransferItem pair for each slot defined on the App.
        """
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return super()._create(**kwargs)

    def all(self) -> "JobQuery":
        """
        Returns a Query for all Job items.
        """
        return self._query_class(manager=self)

    def get(
        self,
        id: Union[typing.List[int], int, None] = None,
        parent_id: Union[typing.List[int], int, None] = None,
        app_id: Optional[int] = None,
        site_id: Union[typing.List[int], int, None] = None,
        batch_job_id: Optional[int] = None,
        last_update_before: Optional[datetime.datetime] = None,
        last_update_after: Optional[datetime.datetime] = None,
        workdir__contains: Optional[str] = None,
        state__ne: Optional[balsam.schemas.job.JobState] = None,
        state: Union[typing.Set[balsam.schemas.job.JobState], balsam.schemas.job.JobState, None] = None,
        tags: Union[typing.List[str], str, None] = None,
        parameters: Union[typing.List[str], str, None] = None,
        pending_file_cleanup: Optional[bool] = None,
    ) -> Job:
        """
        Retrieve exactly one Job. Raises Job.DoesNotExist
        if no items were found, or Job.MultipleObjectsReturned if
        more than one item matched the query.

        id:                   Only return Jobs with ids in this list.
        parent_id:            Only return Jobs that are children of Jobs with ids in this list.
        app_id:               Only return Jobs associated with this App id.
        site_id:              Only return Jobs associated with these Site ids.
        batch_job_id:         Only return Jobs associated with this BatchJob id.
        last_update_before:   Only return Jobs that were updated before this time (UTC).
        last_update_after:    Only return Jobs that were updated after this time (UTC).
        workdir__contains:    Only return jobs with workdirs containing this string.
        state__ne:            Only return jobs with states not equal to this state.
        state:                Only return jobs in this set of states.
        tags:                 Only return jobs containing these tags (list of KEY:VALUE strings)
        parameters:           Only return jobs having these App command parameters (list of KEY:VALUE strings)
        pending_file_cleanup: Only return jobs which have not yet had workdir cleaned.
        """
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return JobQuery(manager=self).get(**kwargs)

    def filter(
        self,
        id: Union[typing.List[int], int, None] = None,
        parent_id: Union[typing.List[int], int, None] = None,
        app_id: Optional[int] = None,
        site_id: Union[typing.List[int], int, None] = None,
        batch_job_id: Optional[int] = None,
        last_update_before: Optional[datetime.datetime] = None,
        last_update_after: Optional[datetime.datetime] = None,
        workdir__contains: Optional[str] = None,
        state__ne: Optional[balsam.schemas.job.JobState] = None,
        state: Union[typing.Set[balsam.schemas.job.JobState], balsam.schemas.job.JobState, None] = None,
        tags: Union[typing.List[str], str, None] = None,
        parameters: Union[typing.List[str], str, None] = None,
        pending_file_cleanup: Optional[bool] = None,
    ) -> "JobQuery":
        """
        Returns a Job Query returning items matching the filter criteria.

        id:                   Only return Jobs with ids in this list.
        parent_id:            Only return Jobs that are children of Jobs with ids in this list.
        app_id:               Only return Jobs associated with this App id.
        site_id:              Only return Jobs associated with these Site ids.
        batch_job_id:         Only return Jobs associated with this BatchJob id.
        last_update_before:   Only return Jobs that were updated before this time (UTC).
        last_update_after:    Only return Jobs that were updated after this time (UTC).
        workdir__contains:    Only return jobs with workdirs containing this string.
        state__ne:            Only return jobs with states not equal to this state.
        state:                Only return jobs in this set of states.
        tags:                 Only return jobs containing these tags (list of KEY:VALUE strings)
        parameters:           Only return jobs having these App command parameters (list of KEY:VALUE strings)
        pending_file_cleanup: Only return jobs which have not yet had workdir cleaned.
        """
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
        """
        Construct a new BatchJob object.  You must eventually call the save() method or
        pass a BatchJob list into BatchJob.objects.bulk_create().

        num_nodes:       Requested number of nodes for this allocation
        wall_time_min:   Requested wall clock time for this allocation
        job_mode:        Balsam launcher execution mode (if single partition)
        optional_params: Optional pass-through parameters submitted with the batchjob script
        filter_tags:     Only run Jobs containing these tags
        partitions:      Optionally, subdivide an allocation into multiple partitions.
        site_id:         The Site id where this batchjob is submitted
        project:         The project/allocation to charge for this batchjob
        queue:           Which queue the batchjob is submitted on
        """
        _kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        _kwargs.update(kwargs)
        return super().__init__(**_kwargs)


class BatchJobQuery(Query[BatchJob]):
    def get(
        self,
        id: Union[typing.List[int], int, None] = None,
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
        """
        Retrieve exactly one BatchJob. Raises BatchJob.DoesNotExist
        if no items were found, or BatchJob.MultipleObjectsReturned if
        more than one item matched the query.

        id:                Only return BatchJobs having an id in this list.
        site_id:           Only return batchjobs for Sites in this id list.
        state:             Only return batchjobs having one of these States in this list.
        scheduler_id:      Return the batchjob with this local scheduler id.
        queue:             Only return batchjobs submitted to this queue.
        start_time_before: Only return batchjobs that started before this time (UTC).
        start_time_after:  Only return batchjobs that started after this time (UTC).
        end_time_before:   Only return batchjobs that finished before this time (UTC).
        end_time_after:    Only return batchjobs that finished after this time (UTC).
        filter_tags:       Only return batchjobs processing these tags (list of KEY:VALUE strings).
        """
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._get(**kwargs)

    def filter(
        self,
        id: Union[typing.List[int], int, None] = None,
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
        """
        Retrieve exactly one BatchJob. Raises BatchJob.DoesNotExist
        if no items were found, or BatchJob.MultipleObjectsReturned if
        more than one item matched the query.

        id:                Only return BatchJobs having an id in this list.
        site_id:           Only return batchjobs for Sites in this id list.
        state:             Only return batchjobs having one of these States in this list.
        scheduler_id:      Return the batchjob with this local scheduler id.
        queue:             Only return batchjobs submitted to this queue.
        start_time_before: Only return batchjobs that started before this time (UTC).
        start_time_after:  Only return batchjobs that started after this time (UTC).
        end_time_before:   Only return batchjobs that finished before this time (UTC).
        end_time_after:    Only return batchjobs that finished after this time (UTC).
        filter_tags:       Only return batchjobs processing these tags (list of KEY:VALUE strings).
        """
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
        """
        Updates all items selected by this query with the given values.

        scheduler_id: The local HPC scheduler's ID for this batchjob
        state:        Status of this batchjob in the local HPC scheduler
        status_info:  Arbitrary status info
        start_time:   BatchJob execution start time
        end_time:     BatchJob execution end time
        """
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._update(**kwargs)

    def order_by(self, field: Optional[balsam.schemas.batchjob.BatchJobOrdering]) -> "BatchJobQuery":
        """
        Order the returned items by this field.
        """
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
        """
        Create a new BatchJob object and save it to the API in one step.

        num_nodes:       Requested number of nodes for this allocation
        wall_time_min:   Requested wall clock time for this allocation
        job_mode:        Balsam launcher execution mode (if single partition)
        optional_params: Optional pass-through parameters submitted with the batchjob script
        filter_tags:     Only run Jobs containing these tags
        partitions:      Optionally, subdivide an allocation into multiple partitions.
        site_id:         The Site id where this batchjob is submitted
        project:         The project/allocation to charge for this batchjob
        queue:           Which queue the batchjob is submitted on
        """
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return super()._create(**kwargs)

    def all(self) -> "BatchJobQuery":
        """
        Returns a Query for all BatchJob items.
        """
        return self._query_class(manager=self)

    def get(
        self,
        id: Union[typing.List[int], int, None] = None,
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
        """
        Retrieve exactly one BatchJob. Raises BatchJob.DoesNotExist
        if no items were found, or BatchJob.MultipleObjectsReturned if
        more than one item matched the query.

        id:                Only return BatchJobs having an id in this list.
        site_id:           Only return batchjobs for Sites in this id list.
        state:             Only return batchjobs having one of these States in this list.
        scheduler_id:      Return the batchjob with this local scheduler id.
        queue:             Only return batchjobs submitted to this queue.
        start_time_before: Only return batchjobs that started before this time (UTC).
        start_time_after:  Only return batchjobs that started after this time (UTC).
        end_time_before:   Only return batchjobs that finished before this time (UTC).
        end_time_after:    Only return batchjobs that finished after this time (UTC).
        filter_tags:       Only return batchjobs processing these tags (list of KEY:VALUE strings).
        """
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return BatchJobQuery(manager=self).get(**kwargs)

    def filter(
        self,
        id: Union[typing.List[int], int, None] = None,
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
        """
        Returns a BatchJob Query returning items matching the filter criteria.

        id:                Only return BatchJobs having an id in this list.
        site_id:           Only return batchjobs for Sites in this id list.
        state:             Only return batchjobs having one of these States in this list.
        scheduler_id:      Return the batchjob with this local scheduler id.
        queue:             Only return batchjobs submitted to this queue.
        start_time_before: Only return batchjobs that started before this time (UTC).
        start_time_after:  Only return batchjobs that started after this time (UTC).
        end_time_before:   Only return batchjobs that finished before this time (UTC).
        end_time_after:    Only return batchjobs that finished after this time (UTC).
        filter_tags:       Only return batchjobs processing these tags (list of KEY:VALUE strings).
        """
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return BatchJobQuery(manager=self).filter(**kwargs)


class Session(balsam._api.bases.SessionBase):
    _create_model_cls = balsam.schemas.session.SessionCreate
    _update_model_cls = None
    _read_model_cls = balsam.schemas.session.SessionOut
    objects: "SessionManager"

    site_id = Field[int]()
    batch_job_id = Field[Optional[int]]()
    id = Field[Optional[int]]()
    heartbeat = Field[Optional[datetime.datetime]]()

    def __init__(
        self,
        site_id: int,
        batch_job_id: Optional[int] = None,
        **kwargs: Any,
    ) -> None:
        """
        Construct a new Session object.  You must eventually call the save() method or
        pass a Session list into Session.objects.bulk_create().

        site_id:      Site id of the running Session
        batch_job_id: Associated batchjob id
        """
        _kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        _kwargs.update(kwargs)
        return super().__init__(**_kwargs)


class SessionQuery(Query[Session]):
    def get(
        self,
        id: Union[typing.List[int], int, None] = None,
    ) -> Session:
        """
        Retrieve exactly one Session. Raises Session.DoesNotExist
        if no items were found, or Session.MultipleObjectsReturned if
        more than one item matched the query.

        id: Only return Sessions having an id in this list.
        """
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._get(**kwargs)

    def filter(
        self,
        id: Union[typing.List[int], int, None] = None,
    ) -> "SessionQuery":
        """
        Retrieve exactly one Session. Raises Session.DoesNotExist
        if no items were found, or Session.MultipleObjectsReturned if
        more than one item matched the query.

        id: Only return Sessions having an id in this list.
        """
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._filter(**kwargs)


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
        """
        Create a new Session object and save it to the API in one step.

        site_id:      Site id of the running Session
        batch_job_id: Associated batchjob id
        """
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return super()._create(**kwargs)

    def all(self) -> "SessionQuery":
        """
        Returns a Query for all Session items.
        """
        return self._query_class(manager=self)

    def get(
        self,
        id: Union[typing.List[int], int, None] = None,
    ) -> Session:
        """
        Retrieve exactly one Session. Raises Session.DoesNotExist
        if no items were found, or Session.MultipleObjectsReturned if
        more than one item matched the query.

        id: Only return Sessions having an id in this list.
        """
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return SessionQuery(manager=self).get(**kwargs)

    def filter(
        self,
        id: Union[typing.List[int], int, None] = None,
    ) -> "SessionQuery":
        """
        Returns a Session Query returning items matching the filter criteria.

        id: Only return Sessions having an id in this list.
        """
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return SessionQuery(manager=self).filter(**kwargs)


class TransferItem(balsam._api.bases.TransferItemBase):
    _create_model_cls = None
    _update_model_cls = balsam.schemas.transfer.TransferItemUpdate
    _read_model_cls = balsam.schemas.transfer.TransferItemOut
    objects: "TransferItemManager"

    state = Field[balsam.schemas.transfer.TransferItemState]()
    task_id = Field[str]()
    transfer_info = Field[typing.Dict[str, typing.Any]]()
    id = Field[int]()
    job_id = Field[int]()
    direction = Field[balsam.schemas.transfer.TransferDirection]()
    local_path = Field[pathlib.Path]()
    remote_path = Field[pathlib.Path]()
    location_alias = Field[str]()
    recursive = Field[bool]()


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
        """
        Retrieve exactly one TransferItem. Raises TransferItem.DoesNotExist
        if no items were found, or TransferItem.MultipleObjectsReturned if
        more than one item matched the query.

        id:        Only return transfer items with IDs in this list.
        site_id:   Only return transfer items associated with this Site id.
        job_id:    Only return transfer items associated with this Job id list.
        state:     Only return transfer items in this set of states.
        direction: Only return items in this transfer direction.
        job_state: Only return transfer items for Jobs having this state.
        tags:      Only return transfer items for Jobs having these tags (list of KEY:VALUE strings).
        """
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
        """
        Retrieve exactly one TransferItem. Raises TransferItem.DoesNotExist
        if no items were found, or TransferItem.MultipleObjectsReturned if
        more than one item matched the query.

        id:        Only return transfer items with IDs in this list.
        site_id:   Only return transfer items associated with this Site id.
        job_id:    Only return transfer items associated with this Job id list.
        state:     Only return transfer items in this set of states.
        direction: Only return items in this transfer direction.
        job_state: Only return transfer items for Jobs having this state.
        tags:      Only return transfer items for Jobs having these tags (list of KEY:VALUE strings).
        """
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._filter(**kwargs)

    def update(
        self,
        state: Optional[balsam.schemas.transfer.TransferItemState] = None,
        task_id: Optional[str] = None,
        transfer_info: Optional[typing.Dict[str, typing.Any]] = None,
    ) -> List[TransferItem]:
        """
        Updates all items selected by this query with the given values.

        state:         Status of this transfer item
        task_id:       Transfer Task ID used to lookup transfer item status
        transfer_info: Arbitrary transfer state info
        """
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
        """
        Returns a Query for all TransferItem items.
        """
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
        """
        Retrieve exactly one TransferItem. Raises TransferItem.DoesNotExist
        if no items were found, or TransferItem.MultipleObjectsReturned if
        more than one item matched the query.

        id:        Only return transfer items with IDs in this list.
        site_id:   Only return transfer items associated with this Site id.
        job_id:    Only return transfer items associated with this Job id list.
        state:     Only return transfer items in this set of states.
        direction: Only return items in this transfer direction.
        job_state: Only return transfer items for Jobs having this state.
        tags:      Only return transfer items for Jobs having these tags (list of KEY:VALUE strings).
        """
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
        """
        Returns a TransferItem Query returning items matching the filter criteria.

        id:        Only return transfer items with IDs in this list.
        site_id:   Only return transfer items associated with this Site id.
        job_id:    Only return transfer items associated with this Job id list.
        state:     Only return transfer items in this set of states.
        direction: Only return items in this transfer direction.
        job_state: Only return transfer items for Jobs having this state.
        tags:      Only return transfer items for Jobs having these tags (list of KEY:VALUE strings).
        """
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return TransferItemQuery(manager=self).filter(**kwargs)


class EventLog(balsam._api.bases.EventLogBase):
    _create_model_cls = None
    _update_model_cls = None
    _read_model_cls = balsam.schemas.logevent.LogEventOut
    objects: "EventLogManager"

    id = Field[int]()
    job_id = Field[int]()
    timestamp = Field[datetime.datetime]()
    from_state = Field[str]()
    to_state = Field[str]()
    data = Field[typing.Dict[str, typing.Any]]()


class EventLogQuery(Query[EventLog]):
    def get(
        self,
        id: Union[typing.List[int], int, None] = None,
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
        """
        Retrieve exactly one EventLog. Raises EventLog.DoesNotExist
        if no items were found, or EventLog.MultipleObjectsReturned if
        more than one item matched the query.

        id:               Only return EventLogs having an id in this list.
        job_id:           Only return Events associated with Job IDs in this list.
        batch_job_id:     Only return Events associated this BatchJob id.
        scheduler_id:     Only return Events associated with this HPC scheduler job ID.
        tags:             Only return Events for Jobs containing these tags (list of KEY:VALUE strings)
        data:             Only return Events containing this data (list of KEY:VALUE strings)
        timestamp_before: Only return Events before this time (UTC).
        timestamp_after:  Only return Events that occured after this time (UTC).
        from_state:       Only return Events transitioning from this Job state.
        to_state:         Only return Events transitioning to this Job state.
        """
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._get(**kwargs)

    def filter(
        self,
        id: Union[typing.List[int], int, None] = None,
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
        """
        Retrieve exactly one EventLog. Raises EventLog.DoesNotExist
        if no items were found, or EventLog.MultipleObjectsReturned if
        more than one item matched the query.

        id:               Only return EventLogs having an id in this list.
        job_id:           Only return Events associated with Job IDs in this list.
        batch_job_id:     Only return Events associated this BatchJob id.
        scheduler_id:     Only return Events associated with this HPC scheduler job ID.
        tags:             Only return Events for Jobs containing these tags (list of KEY:VALUE strings)
        data:             Only return Events containing this data (list of KEY:VALUE strings)
        timestamp_before: Only return Events before this time (UTC).
        timestamp_after:  Only return Events that occured after this time (UTC).
        from_state:       Only return Events transitioning from this Job state.
        to_state:         Only return Events transitioning to this Job state.
        """
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._filter(**kwargs)

    def order_by(self, field: Optional[balsam.schemas.logevent.EventOrdering]) -> "EventLogQuery":
        """
        Order the returned items by this field.
        """
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
        """
        Returns a Query for all EventLog items.
        """
        return self._query_class(manager=self)

    def get(
        self,
        id: Union[typing.List[int], int, None] = None,
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
        """
        Retrieve exactly one EventLog. Raises EventLog.DoesNotExist
        if no items were found, or EventLog.MultipleObjectsReturned if
        more than one item matched the query.

        id:               Only return EventLogs having an id in this list.
        job_id:           Only return Events associated with Job IDs in this list.
        batch_job_id:     Only return Events associated this BatchJob id.
        scheduler_id:     Only return Events associated with this HPC scheduler job ID.
        tags:             Only return Events for Jobs containing these tags (list of KEY:VALUE strings)
        data:             Only return Events containing this data (list of KEY:VALUE strings)
        timestamp_before: Only return Events before this time (UTC).
        timestamp_after:  Only return Events that occured after this time (UTC).
        from_state:       Only return Events transitioning from this Job state.
        to_state:         Only return Events transitioning to this Job state.
        """
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return EventLogQuery(manager=self).get(**kwargs)

    def filter(
        self,
        id: Union[typing.List[int], int, None] = None,
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
        """
        Returns a EventLog Query returning items matching the filter criteria.

        id:               Only return EventLogs having an id in this list.
        job_id:           Only return Events associated with Job IDs in this list.
        batch_job_id:     Only return Events associated this BatchJob id.
        scheduler_id:     Only return Events associated with this HPC scheduler job ID.
        tags:             Only return Events for Jobs containing these tags (list of KEY:VALUE strings)
        data:             Only return Events containing this data (list of KEY:VALUE strings)
        timestamp_before: Only return Events before this time (UTC).
        timestamp_after:  Only return Events that occured after this time (UTC).
        from_state:       Only return Events transitioning from this Job state.
        to_state:         Only return Events transitioning to this Job state.
        """
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return EventLogQuery(manager=self).filter(**kwargs)
