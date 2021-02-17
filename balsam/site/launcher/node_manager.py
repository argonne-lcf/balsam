from logging import getLogger
from typing import TYPE_CHECKING, Any, Dict, List

from pydantic import BaseModel, validator

from balsam._api.models import Job

if TYPE_CHECKING:
    from balsam.platform.compute_node import ComputeNode

logger = getLogger(__name__)


class InsufficientResources(Exception):
    pass


class NodeSpec(BaseModel):
    node_ids: List[str]
    hostnames: List[str]
    cpu_ids: List[List[int]] = []
    gpu_ids: List[List[str]] = []

    @validator("hostnames")
    def hostnames_len(cls, v: List[str], values: Dict[str, Any]) -> List[str]:
        if len(values["node_ids"]) != len(v):
            raise ValueError("Must provide same number of node_ids as hostnames")
        return v

    @validator("cpu_ids", always=True)
    def cpu_ids_len(cls, v: List[List[int]], values: Dict[str, Any]) -> List[List[int]]:
        if not v:
            v = [[] for _ in range(len(values["node_ids"]))]
        elif len(values["node_ids"]) != len(v):
            raise ValueError("Must provide same number of cpu_id lists")
        return v

    @validator("gpu_ids", always=True)
    def gpu_ids_len(cls, v: List[List[str]], values: Dict[str, Any]) -> List[List[str]]:
        if not v:
            v = [[] for _ in range(len(values["node_ids"]))]
        elif len(values["node_ids"]) != len(v):
            raise ValueError("Must provide same number of gpu_id lists")
        return v


class NodeManager:
    def __init__(self, node_list: "List[ComputeNode]", allow_node_packing: bool = True) -> None:
        self.nodes = node_list
        self.job_node_map: Dict[int, List[int]] = {}
        self.allow_node_packing = allow_node_packing

    def _assign_single_node(self, job_id: int, num_cpus: int, num_gpus: int, node_occupancy: float) -> NodeSpec:
        logger.debug(f"Assigning job {job_id}: {num_cpus} CPU, {num_gpus} GPU, {node_occupancy} occupancy")
        logger.debug(f"Current resources:{self.nodes}")
        if not self.allow_node_packing:
            node_occupancy = 1.0
        for node_idx, node in enumerate(self.nodes):
            if node.check_fit(num_cpus, num_gpus, node_occupancy):
                spec = node.assign(job_id, num_cpus, num_gpus, node_occupancy)
                self.job_node_map[job_id] = [node_idx]
                return NodeSpec(
                    node_ids=[node.node_id],
                    hostnames=[node.hostname],
                    cpu_ids=[spec["cpu_ids"]],
                    gpu_ids=[spec["gpu_ids"]],
                )
        raise InsufficientResources

    def _assign_multi_node(self, job_id: int, num_nodes: int) -> NodeSpec:
        assigned_nodes = []
        assigned_idxs = []
        for node_idx, node in enumerate(self.nodes):
            if node.check_fit(num_cpus=0, num_gpus=0, occupancy=1.0):
                assigned_nodes.append(node)
                assigned_idxs.append(node_idx)
            if len(assigned_nodes) == num_nodes:
                break
        else:
            raise InsufficientResources
        node_ids, hostnames = [], []
        for node in assigned_nodes:
            node.assign(job_id, num_cpus=0, num_gpus=0, occupancy=1.0)
            node_ids.append(node.node_id)
            hostnames.append(node.hostname)
        self.job_node_map[job_id] = assigned_idxs
        return NodeSpec(node_ids=node_ids, hostnames=hostnames)

    def count_empty_nodes(self) -> int:
        return len([node for node in self.nodes if node.occupancy == 0.0])

    def aggregate_free_nodes(self) -> float:
        return float(len(self.nodes)) - sum(n.occupancy for n in self.nodes)

    def assign(self, job: Job) -> NodeSpec:
        assert job.id is not None
        return self.assign_from_params(
            id=job.id,
            num_nodes=job.num_nodes,
            ranks_per_node=job.ranks_per_node,
            threads_per_rank=job.threads_per_rank,
            threads_per_core=job.threads_per_core,
            gpus_per_rank=job.gpus_per_rank,
            node_occupancy=1.0 / job.node_packing_count,
        )

    def assign_from_params(
        self,
        id: int,
        num_nodes: int,
        ranks_per_node: int,
        threads_per_rank: int,
        threads_per_core: int,
        gpus_per_rank: float,
        node_occupancy: float,
        **kwargs: Any,
    ) -> NodeSpec:
        if num_nodes > 1:
            return self._assign_multi_node(id, num_nodes)
        num_cpus = max(1, int(ranks_per_node * threads_per_rank // threads_per_core))
        num_gpus = int(ranks_per_node * gpus_per_rank)
        return self._assign_single_node(id, num_cpus, num_gpus, node_occupancy)

    def free(self, job_id: int) -> None:
        node_idxs = self.job_node_map.pop(job_id)
        for idx in node_idxs:
            node = self.nodes[idx]
            node.free(job_id)
