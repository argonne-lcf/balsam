import os
from compute_node import ComputeNode


class CoriKnlNode(ComputeNode):

    num_cpu = 68
    num_gpu = 0
    cpu_identifiers = list(range(num_cpu))
    gpu_identifiers = list(range(num_gpu))
    allow_multi_mpirun = True
    cpu_type = "Intel Xeon Phi 7250"
    cpu_mem_gb = 94
    gpu_type = ""
    gpu_mem_gb = 0

    def __init__(self, node_id, hostname, job_mode=""):
        super(CoriKnlNode, self).__init__(node_id, hostname, job_mode)

    def __str__(self):
        return f"{self.hostname}:cpu{self.num_cpu}:gpu{self.num_gpu}"

    @classmethod
    def get_job_nodelist(cls):
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
        for node_range_str in node_ranges_str:
            lo, *hi = node_range_str.split("-")
            lo = int(lo)
            if hi:
                hi = int(hi[0])
                node_ids.extend(list(range(lo, hi + 1)))
            else:
                node_ids.append(lo)

        return [cls(node_id, f"nid{node_id:05d}") for node_id in node_ids]


if __name__ == "__main__":

    if "SLURM_NODELIST" not in os.environ:
        os.environ["SLURM_NODELIST"] = "nid0[3038-3039,8241-8246]"

    print([str(x) for x in CoriKnlNode.get_job_nodelist()])
