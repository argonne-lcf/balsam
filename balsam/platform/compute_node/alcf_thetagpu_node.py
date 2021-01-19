import os
from .compute_node import ComputeNode


class ThetaGpuNode(ComputeNode):

    cpu_ids = list(range(128))
    gpu_ids = list(range(8))
    allow_multi_mpirun = True

    @classmethod
    def get_job_nodelist(cls):
        """
        Get all compute nodes allocated in the current job context
        """
        nodefile = os.environ["COBALT_NODEFILE"]
        # a file containing a list of node hostnames, one per line
        # thetagpu01
        # thetagpu02

        nodefile_lines = open(nodefile).readlines()
        node_hostnames = [line.strip() for line in nodefile_lines]
        node_ids = [int(hostname[-2:]) for hostname in node_hostnames]
        return [cls(node_ids[i], node_hostnames[i]) for i in range(len(node_ids))]
