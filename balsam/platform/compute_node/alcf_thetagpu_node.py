import os
from typing import List, Optional

from .compute_node import ComputeNode


class ThetaGPUNode(ComputeNode):

    cpu_ids = list(range(128))
    gpu_ids = list(range(8))

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
        return [cls(nid, hostname) for nid, hostname in zip(node_ids, hostnames)]

    @staticmethod
    def get_scheduler_id() -> Optional[int]:
        id = os.environ.get("COBALT_JOBID")
        if id is not None:
            return int(id)
        return None
