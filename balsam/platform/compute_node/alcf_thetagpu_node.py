import logging
import os
from pathlib import Path
from typing import List, Optional, Union

from .compute_node import ComputeNode

logger = logging.getLogger(__name__)
IntStr = Union[int, str]


class ThetaGPUNode(ComputeNode):

    cpu_ids = list(range(128))
    gpu_ids: List[IntStr] = list(range(8))

    @classmethod
    def get_job_nodelist(cls) -> List["ThetaGPUNode"]:
        """
        Get all compute nodes allocated in the current job context
        """
        nodefile = os.environ["COBALT_NODEFILE"]
        # a file containing a list of node hostnames, one per line
        # thetagpu01
        # thetagpu02
        with open(nodefile) as fp:
            data = fp.read()
        splitter = "," if "," in data else None
        hostnames = data.split(splitter)
        hostnames = [h.strip() for h in hostnames if h.strip()]
        node_ids = [int(hostname[-2:]) for hostname in hostnames]
        node_list = []
        for nid, hostname in zip(node_ids, hostnames):
            gpu_ids = cls.discover_gpu_list(hostname)
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
        id = os.environ.get("COBALT_JOBID")
        if id is not None:
            return int(id)
        return None
