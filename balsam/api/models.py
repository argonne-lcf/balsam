from pathlib import Path
from typing import List, Dict, Set, Optional
from .model_base import BalsamModel
from .manager_base import Manager
from balsam import schemas

AppParameter = schemas.AppParameter
TransferSlot = schemas.TransferSlot
JobState = schemas.JobState
RUNNABLE_STATES = schemas.RUNNABLE_STATES


class SiteManager(Manager):
    path = "sites/"


class AppManager(Manager):
    path = "apps/"


class BatchJobManager(Manager):
    path = "batch-jobs/"
    bulk_update_enabled = True


class JobManager(Manager):
    path = "jobs/"
    bulk_create_enabled = True
    bulk_update_enabled = True
    bulk_delete_enabled = True


class TransferManager(Manager):
    path = "transfers/"
    bulk_update_enabled = True


class SessionManager(Manager):
    path = "sessions/"

    def _do_acquire(self, instance, **kwargs):
        acquired_raw = self._client.post(self.path + f"{instance.id}", **kwargs)
        jobs = [Job.from_api(dat) for dat in acquired_raw]
        return jobs

    def _do_tick(self, instance):
        self._client.put(self.path + f"{instance.id}")


class EventLogManager(Manager):
    path = "events/"


class Site(BalsamModel):
    create_model_cls = schemas.SiteCreate
    update_model_cls = schemas.SiteUpdate
    read_model_cls = schemas.SiteOut
    objects = SiteManager()


class App(BalsamModel):
    create_model_cls = schemas.AppCreate
    update_model_cls = schemas.AppUpdate
    read_model_cls = schemas.AppOut
    objects = AppManager()

    def __init__(
        self,
        site_id: int = None,
        class_path: str = None,
        parameters: Dict[str, AppParameter] = None,
        transfers: Dict[str, TransferSlot] = None,
        description: str = "",
        **kwargs,
    ):
        if transfers is None:
            transfers = {}
        return super().__init__(
            site_id=site_id,
            class_path=class_path,
            parameters=parameters if parameters else {},
            transfers=transfers,
            description=description,
            **kwargs,
        )


class BatchJob(BalsamModel):
    create_model_cls = schemas.BatchJobCreate
    update_model_cls = schemas.BatchJobUpdate
    read_model_cls = schemas.BatchJobOut
    objects = BatchJobManager()


class Job(BalsamModel):
    create_model_cls = schemas.JobCreate
    update_model_cls = schemas.JobUpdate
    read_model_cls = schemas.JobOut
    objects = JobManager()

    def __init__(
        self,
        workdir: Path,
        app_id: int,
        tags: Dict[str, str] = None,
        parameters: Dict[str, str] = None,
        parent_ids: List[int] = None,
        transfers: Dict[str, str] = None,
        data: Dict[str, str] = None,
        num_nodes: int = 1,
        ranks_per_node: int = 1,
        threads_per_rank: int = 1,
        threads_per_core: int = 1,
        launch_params: Dict[str, str] = None,
        gpus_per_rank: float = 0.0,
        node_packing_count: int = 1,
        wall_time_min: int = 0,
        **kwargs,
    ):
        return super().__init__(
            workdir=workdir,
            app_id=app_id,
            tags=tags if tags else {},
            parameters=parameters if parameters else {},
            parent_ids=parent_ids if parent_ids else [],
            transfers=transfers if transfers else {},
            data=data if data else {},
            num_nodes=num_nodes,
            ranks_per_node=ranks_per_node,
            threads_per_rank=threads_per_rank,
            threads_per_core=threads_per_core,
            launch_params=launch_params if launch_params else {},
            gpus_per_rank=gpus_per_rank,
            node_packing_count=node_packing_count,
            wall_time_min=wall_time_min,
            **kwargs,
        )

    def history(self):
        return EventLog.objects.filter(job_id=self.id)


class Transfer(BalsamModel):
    create_model_cls = None
    update_model_cls = schemas.TransferItemUpdate
    read_model_cls = schemas.TransferItemOut
    objects = TransferManager()


class Session(BalsamModel):
    create_model_cls = schemas.SessionCreate
    update_model_cls = None
    read_model_cls = schemas.SessionOut
    objects = SessionManager()

    def acquire_jobs(
        self,
        max_num_jobs: int,
        max_wall_time_min: int,
        max_nodes_per_job: Optional[int] = None,
        max_aggregate_nodes: Optional[float] = None,
        serial_only: bool = False,
        filter_tags: Dict[str, str] = None,
        states: Set[JobState] = RUNNABLE_STATES,
    ):
        if filter_tags is None:
            filter_tags = {}
        return self.__class__.objects._do_acquire(
            self,
            max_num_jobs=max_num_jobs,
            max_wall_time_min=max_wall_time_min,
            max_nodes_per_job=max_nodes_per_job,
            max_aggregate_nodes=max_aggregate_nodes,
            serial_only=serial_only,
            filter_tags=filter_tags,
            states=states,
        )

    def tick(self):
        return self.__class__.objects._do_tick(self)


class EventLog(BalsamModel):
    create_model_cls = None
    update_model_cls = None
    read_model_cls = schemas.LogEventOut
    objects = EventLogManager()
