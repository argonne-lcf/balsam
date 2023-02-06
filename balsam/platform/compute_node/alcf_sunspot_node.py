import logging
import os
from typing import List, Optional, Union

from .compute_node import ComputeNode

logger = logging.getLogger(__name__)
IntStr = Union[int, str]


class SunspotNode(ComputeNode):
    cpu_ids = list(range(104))
    gpu_ids: List[IntStr]

    gpu_ids = []
    for gid in range(6):
        for tid in range(2):
            gpu_ids.append(str(gid) + "." + str(tid))

    @classmethod
    def get_job_nodelist(cls) -> List["SunspotNode"]:
        """
        Get all compute nodes allocated in the current job context
        """
        nodefile = os.environ["PBS_NODEFILE"]
        # a file containing a list of node hostnames, one per line
        # thetagpu01
        # thetagpu02
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
        gpu_ids = cls.gpu_ids
        logger.info(f"{hostname} detected GPU IDs: {gpu_ids}")
        return gpu_ids

    @staticmethod
    def get_scheduler_id() -> Optional[int]:
        id = os.environ.get("PBS_JOBID")
        if id is not None:
            return int(id.split(".")[0])
        return None
