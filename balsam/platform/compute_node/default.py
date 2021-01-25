import psutil
from .compute_node import ComputeNode


class DefaultNode(ComputeNode):

    cpu_ids = list(range(psutil.cpu_count() or 4))
    gpu_ids = []

    @classmethod
    def get_job_nodelist(cls):
        return [cls("localhost", "localhost")]
