import logging
import os
from pathlib import Path
from typing import List, Union, Optional

from balsam.platform.compute_node import ComputeNode

logger = logging.getLogger(__name__)
IntStr = Union[int, str]


class SophiaNode(ComputeNode):
    # Replace these with the actual number of CPUs and GPUs per node on Sophia
    cpu_ids = list(range(128))       # 128 CPUs per node  # How do we add hyperthreading?
    gpu_ids: List[IntStr] = list(range(8))  # Example: 8 GPUs per node

    @classmethod
    def get_job_nodelist(cls) -> List["SophiaNode"]:
        """
        Get all compute nodes allocated in the current job context on Sophia
        """
        nodefile = os.environ.get("PBS_NODEFILE")
        if not nodefile:
            raise EnvironmentError("PBS_NODEFILE environment variable is not set.")
        with open(nodefile) as fp:
            data = fp.read()
        splitter = "," if "," in data else None
        hostnames = data.split(splitter)
        hostnames = [h.strip() for h in hostnames if h.strip()]
        node_ids: Union[List[str], List[int]]
        node_ids = hostnames[:]
        node_list = []
        for nid, hostname in zip(node_ids, hostnames):
            gpu_ids = cls.discover_gpu_list(hostname)
            assert isinstance(nid, str) or isinstance(nid, int)
            node_list.append(cls(nid, hostname, gpu_ids=gpu_ids))
        return node_list

    @classmethod
    def discover_gpu_list(cls, hostname: str) -> List[IntStr]:
        gpu_file = Path(f"/var/tmp/balsam-{hostname}-gpulist.txt")
        gpu_ids: List[IntStr]
        if gpu_file.is_file():
            tokens = gpu_file.read_text().split()
            gpu_ids = [t[:-1] for t in tokens if t.startswith("MIG-GPU-")]
        else:
            gpu_ids = cls.gpu_ids
        logger.info(f"{hostname} detected GPU IDs: {gpu_ids}")
        return gpu_ids

    @staticmethod
    def get_scheduler_id() -> Optional[int]:
        job_id = os.environ.get("PBS_JOBID")
        if job_id is not None:
            # PBS_JOBID might include a ".hostname" suffix; strip it off
            return int(job_id.split('.')[0])
        return None
