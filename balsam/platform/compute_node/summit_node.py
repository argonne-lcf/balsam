import os
from .compute_node import ComputeNode


class SummitNode(ComputeNode):

    cpu_ids = list(range(42))
    gpu_ids = list(range(6))
    allow_multi_mpirun = True

    @classmethod
    def get_job_nodelist(cls):
        """
        Get all compute nodes allocated in the current job context
        """
        nodefile = os.environ["LSB_DJOB_HOSTFILE"]
        # a file containing a list of node hostnames, one per line
        # thetagpu01
        # thetagpu02

        nodefile_lines = open(nodefile).readlines()
        node_hostnames = [line.strip() for line in nodefile_lines]
        node_ids = [int(hostname[-2:]) for hostname in node_hostnames]
        return [cls(node_ids[i], node_hostnames[i]) for i in range(len(node_ids))]

    @staticmethod
    def get_batch_job_id():
        id = os.environ.get("LSB_JOBID")
        if id is not None:
            return int(id)
        return None
