import socket
from typing import List, Union

import psutil  # type: ignore

from .compute_node import ComputeNode


class DefaultNode(ComputeNode):

    cpu_ids = list(range(psutil.cpu_count() or 4))
    gpu_ids: List[Union[int, str]] = []

    @classmethod
    def get_job_nodelist(cls, job_mode: str) -> List["DefaultNode"]:
        hostname = socket.gethostname()
        return [cls(hostname, hostname)]
