from typing import Any, Dict, List, Optional, Type, TypeVar, Union

IntStr = Union[int, str]

U = TypeVar("U")


class ComputeNode:

    cpu_ids: List[IntStr] = []
    gpu_ids: List[IntStr] = []

    def __init__(self, node_id: IntStr, hostname: str, gpu_ids: Optional[List[IntStr]] = None) -> None:
        self.node_id = node_id
        self.hostname = hostname
        self.occupancy = 0.0
        self.jobs: Dict[int, Dict[str, Any]] = {}
        self.idle_cpus: List[IntStr] = [i for i in self.cpu_ids]
        self.busy_cpus: List[IntStr] = []
        if gpu_ids is None:
            gpu_ids = self.gpu_ids
        self.idle_gpus: List[IntStr] = [i for i in gpu_ids]
        self.busy_gpus: List[IntStr] = []

    def check_fit(self, num_cpus: int, num_gpus: int, occupancy: float) -> bool:
        if self.occupancy + occupancy > 1.001:
            return False
        elif num_cpus > len(self.idle_cpus):
            return False
        elif num_gpus > len(self.idle_gpus):
            return False
        else:
            return True

    def assign(self, job_id: int, num_cpus: int = 0, num_gpus: int = 0, occupancy: float = 1.0) -> Dict[str, Any]:
        if job_id in self.jobs:
            raise ValueError(f"Already have job {job_id}")

        self.occupancy += occupancy
        if self.occupancy > 0.999:
            self.occupancy = 1.0

        assigned_cpus = self.idle_cpus[:num_cpus]
        assigned_gpus = self.idle_gpus[:num_gpus]
        if assigned_cpus:
            self.busy_cpus.extend(assigned_cpus)
            self.idle_cpus = [i for i in self.idle_cpus if i not in assigned_cpus]
        if assigned_gpus:
            self.busy_gpus.extend(assigned_gpus)
            self.idle_gpus = [i for i in self.idle_gpus if i not in assigned_gpus]
        resource_spec = {
            "cpu_ids": assigned_cpus,
            "gpu_ids": assigned_gpus,
            "occupancy": occupancy,
        }
        self.jobs[job_id] = resource_spec
        return resource_spec

    def free(self, job_id: int) -> None:
        resource_spec = self.jobs.pop(job_id)
        self.occupancy -= resource_spec["occupancy"]
        if self.occupancy < 0.001:
            self.occupancy = 0.0
        cpu_ids = resource_spec["cpu_ids"]
        gpu_ids = resource_spec["gpu_ids"]
        if cpu_ids:
            self.idle_cpus.extend(cpu_ids)
            self.busy_cpus = [i for i in self.busy_cpus if i not in cpu_ids]
        if gpu_ids:
            self.idle_gpus.extend(gpu_ids)
            self.busy_gpus = [i for i in self.busy_gpus if i not in gpu_ids]

    @classmethod
    def get_job_nodelist(cls: Type[U]) -> "List[U]":
        """
        Get all compute nodes allocated in the current job context
        """
        return []

    @staticmethod
    def get_scheduler_id() -> Optional[int]:
        return None

    def __repr__(self) -> str:
        busy_cpus = len(self.busy_cpus)
        total_cpus = len(self.cpu_ids)
        busy_gpus = len(self.busy_gpus)
        total_gpus = len(self.gpu_ids)
        cpu_str = f"{busy_cpus}/{total_cpus} CPUs busy"
        gpu_str = f", {busy_gpus}/{total_gpus} GPUs busy" if total_gpus else ""
        d = dict(
            node_id=self.node_id,
            hostname=self.hostname,
            occupancy=self.occupancy,
            num_jobs=len(self.jobs),
        )
        args = ", ".join(f"{k}={v}" for k, v in d.items())
        rep = f"{self.__class__.__name__}({args}, {cpu_str}{gpu_str})"
        return rep
