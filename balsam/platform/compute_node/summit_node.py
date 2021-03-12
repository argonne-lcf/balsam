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

        nodefile_lines = open(nodefile).readlines()
        # remove new line chars
        node_hostnames = [line.strip() for line in nodefile_lines]
        # deduplicate
        node_hostnames = list(set(node_hostnames))
        # remove batch#
        new_list = []
        for entry in node_hostnames:
            if 'batch' not in entry:
                new_list.append(entry)
        node_hostnames = new_list
        return [cls(host, host) for host in node_hostnames]

    @staticmethod
    def get_scheduler_id() -> Optional[int]:
        id = os.environ.get("LSB_JOBID")
        if id is not None:
            return int(id)
        return None
