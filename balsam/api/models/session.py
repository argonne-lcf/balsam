from typing import Dict, Optional, Set

from balsam import schemas
from balsam.api.manager import Manager
from balsam.api.model import BalsamModel

from .job import Job

JobState = schemas.JobState
RUNNABLE_STATES = schemas.RUNNABLE_STATES


class Session(BalsamModel):
    _create_model_cls = schemas.SessionCreate
    _update_model_cls = None
    _read_model_cls = schemas.SessionOut

    def acquire_jobs(
        self,
        max_num_jobs: int,
        max_wall_time_min: Optional[int] = None,
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


class SessionManager(Manager):
    path = "sessions/"
    _model_class = Session

    def _do_acquire(self, instance, **kwargs):
        acquired_raw = self._client.post(self.path + f"{instance.id}", **kwargs)
        jobs = [Job._from_api(dat) for dat in acquired_raw]
        return jobs

    def _do_tick(self, instance):
        self._client.put(self.path + f"{instance.id}")
