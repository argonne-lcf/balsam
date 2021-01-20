from pydantic import BaseModel, validator
from typing import List


class InsufficientResources(Exception):
    pass


class NodeSpec(BaseModel):
    node_ids: List[str]
    hostnames: List[str]
    cpu_ids: List[List[int]] = []
    gpu_ids: List[List[str]] = []

    @validator("hostnames")
    def hostnames_len(cls, v, values):
        if len(values["node_ids"]) != len(v):
            raise ValueError("Must provide same number of node_ids as hostnames")
        return v

    @validator("cpu_ids")
    def cpu_ids_len(cls, v, values):
        if not v:
            v = [[] for _ in range(len(values["node_ids"]))]
        elif len(values["node_ids"]) != len(v):
            raise ValueError("Must provide same number of cpu_id lists")
        return v

    @validator("gpu_ids")
    def gpu_ids_len(cls, v, values):
        if not v:
            v = [[] for _ in range(len(values["node_ids"]))]
        elif len(values["node_ids"]) != len(v):
            raise ValueError("Must provide same number of gpu_id lists")
        return v


class NodeManager:
    def __init__(self, node_list, allow_node_packing=True):
        self.nodes = node_list
        self.job_node_map = {}
        self.allow_node_packing = allow_node_packing

    def _assign_single_node(self, job_id, num_cpus, num_gpus, node_occupancy):
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

    def _assign_multi_node(self, job_id, num_nodes):
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
            node_ids.append(node.id)
            hostnames.append(node.hostname)
        self.job_node_map[job_id] = assigned_idxs
        return NodeSpec(node_ids=node_ids, hostnames=hostnames)

    def assign(self, job_id, num_nodes, cpus_per_node, gpus_per_node, node_occupancy):
        if num_nodes > 1:
            return self._assign_multi_node(job_id, num_nodes)
        return self._assign_single_node(
            job_id, cpus_per_node, gpus_per_node, node_occupancy
        )

    def free(self, job_id):
        node_idxs = self.job_node_map.pop(job_id)
        for idx in node_idxs:
            node = self.nodes[idx]
            node.free(job_id)
