import os
from typing import List, Optional

from .compute_node import ComputeNode


class CooleyNode(ComputeNode):

    cpu_ids = list(range(12))
    gpu_ids = list(range(2))

    @classmethod
    def get_job_nodelist(cls) -> List["CooleyNode"]:
        """
        Get all compute nodes allocated in the current job context
        """
        nodefile = os.environ["COBALT_NODEFILE"]
        # a file containing a list of node hostnames, one per line
        with open(nodefile) as fp:
            data = fp.read()
        splitter = "," if "," in data else None
        hostnames = data.split(splitter)
        hostnames = [h.strip().split(".")[0] for h in hostnames if h.strip()]
        return [cls(hostname, hostname) for hostname in hostnames]

    @staticmethod
    def get_scheduler_id() -> Optional[int]:
        id = os.environ.get("COBALT_JOBID")
        if id is not None:
            return int(id)
        return None
