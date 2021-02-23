import logging
import socket
from typing import List, Optional, Union

import psutil  # type: ignore

from .compute_node import ComputeNode

logger = logging.getLogger(__name__)


class DefaultNode(ComputeNode):

    cpu_ids = list(range(psutil.cpu_count() or 4))
    gpu_ids: List[Union[int, str]] = []

    @classmethod
    def get_job_nodelist(cls) -> List["DefaultNode"]:
        hostname = socket.gethostname()
        return [cls(hostname, hostname)]

    @staticmethod
    def get_scheduler_id() -> Optional[int]:
        # The parent shell script is tracked by the local scheduler:
        for parent in psutil.Process().parents():
            cmdline = " ".join(parent.cmdline())
            if "bash" in cmdline and "qlaunch" in cmdline:
                pid = int(parent.pid)
                logger.info(f"Detected scheduled job [{pid}]: {cmdline}")
                return pid
        logger.info("Could not detected scheduler_id of this process.")
        return None
