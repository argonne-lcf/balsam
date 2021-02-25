import os
from typing import List, Optional, Union

from .compute_node import ComputeNode


class CoriHaswellNode(ComputeNode):

    cpu_ids = list(range(32))
    gpu_ids: List[Union[int, str]] = []

    @classmethod
    def get_job_nodelist(cls) -> List["CoriHaswellNode"]:
        """
        Get all compute nodes allocated in the current job context
        """
        nodelist_str = os.environ["SLURM_NODELIST"]
        # string like: nid[02529,02878,03047,03290,03331,03813,11847-11848]
        # or like: nid0[3038-3039,8241-8246]
        # or like: nid00[858-861]
        # or like: nid000[10-11]
        # or like: nid00558

        # first remove 'nid', '[' and ']'
        nodelist_str = nodelist_str.replace("nid", "").replace("[", "").replace("]", "")
        # now have something like '02529,02878,03047,03290,03331,03813,11847-11848'
        # split by comma
        node_ranges_str = nodelist_str.split(",")
        node_ids = []
        lo: Union[str, int]
        hi: Union[int, List[str]]
        for node_range_str in node_ranges_str:
            lo, *hi = node_range_str.split("-")
            lo = int(lo)
            if hi:
                hi = int(hi[0])
                node_ids.extend(list(range(lo, hi + 1)))
            else:
                node_ids.append(lo)

        return [cls(node_id, f"nid{node_id:05d}") for node_id in node_ids]

    @staticmethod
    def get_scheduler_id() -> Optional[int]:
        id = os.environ.get("SLURM_JOB_ID")
        if id is not None:
            return int(id)
        return None


if __name__ == "__main__":
    if "SLURM_NODELIST" not in os.environ:
        os.environ["SLURM_NODELIST"] = "nid0[3038-3039,8241-8246]"
    print([str(x) for x in CoriHaswellNode.get_job_nodelist()])
