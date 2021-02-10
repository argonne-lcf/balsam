import os
from typing import List, Optional, Union

from .compute_node import ComputeNode


class ThetaKNLNode(ComputeNode):

    cpu_ids = list(range(64))
    gpu_ids: List[Union[int, str]] = []

    @classmethod
    def get_job_nodelist(cls) -> List["ThetaKNLNode"]:
        """
        Get all compute nodes allocated in the current job context
        """
        node_str = os.environ["COBALT_PARTNAME"]
        # string like: 1001-1005,1030,1034-1200
        node_ids = []
        ranges = node_str.split(",")
        lo: Union[str, int]
        hi: Union[int, List[str]]
        for node_range in ranges:
            lo, *hi = node_range.split("-")
            lo = int(lo)
            if hi:
                hi = int(hi[0])
                node_ids.extend(list(range(lo, hi + 1)))
            else:
                node_ids.append(lo)

        return [cls(node_id, f"nid{node_id:05d}") for node_id in node_ids]

    @staticmethod
    def get_scheduler_id() -> Optional[int]:
        id = os.environ.get("COBALT_JOBID")
        if id is not None:
            return int(id)
        return None
