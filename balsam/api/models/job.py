from pathlib import Path
from typing import Dict, List, Optional

from balsam import schemas
from balsam.api.manager_base import Manager
from balsam.api.model_base import BalsamModel
from balsam.api.query import Query


class Job(BalsamModel):
    create_model_cls = schemas.JobCreate
    update_model_cls = schemas.JobUpdate
    read_model_cls = schemas.JobOut
    objects: "JobManager"

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


class JobQuery(Query["Job"]):
    def get(self, id: Optional[int] = None) -> Job:
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._get(**kwargs)

    def filter(self, id: Optional[int] = None) -> "JobQuery":
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._filter(**kwargs)

    def update(self, num_nodes: Optional[int] = None) -> List[Job]:
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._update(**kwargs)

    def order_by(self, *fields: str) -> "JobQuery":
        return self._order_by(*fields)


class JobManager(Manager[Job]):
    path = "jobs/"
    bulk_create_enabled = True
    bulk_update_enabled = True
    bulk_delete_enabled = True
    model_class = Job

    def filter(self, id: Optional[int] = None) -> "JobQuery":
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return JobQuery(manager=self).filter(**kwargs)

    def get(self, id: Optional[int] = None) -> Job:
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return JobQuery(manager=self).get(**kwargs)

    def create(self, workdir: str) -> Job:
        return self._create(workdir=workdir)
