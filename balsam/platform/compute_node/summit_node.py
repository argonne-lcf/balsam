import os
from typing import List, Optional

from .compute_node import ComputeNode


class SummitNode(ComputeNode):

    cpu_ids = list(range(42))
    gpu_ids = list(range(6))

    @classmethod
    def get_job_nodelist(cls) -> List["SummitNode"]:
        """
        Get all compute nodes allocated in the current job context
        """
        nodefile = os.environ["LSB_DJOB_HOSTFILE"]
        # a file containing a list of node hostnames, one per line
        # batch3
        # a01n06
        # a01n06
        # ... 1 per CPU core

        # Ignore whitespace, de-duplicate, ignore 'batch' node
        with open(nodefile) as fp:
            node_hostnames = set(line.strip() for line in fp if line.strip() and "batch" not in line)
        return [cls(host, host) for host in node_hostnames]

    @staticmethod
    def get_scheduler_id() -> Optional[int]:
        id = os.environ.get("LSB_JOBID")
        if id is not None:
            return int(id)
        return None
