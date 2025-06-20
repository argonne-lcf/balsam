import logging
import os
from typing import Any, Dict, List, Optional, Union

from .compute_node import ComputeNode

logger = logging.getLogger(__name__)
IntStr = Union[int, str]


class AuroraNode(ComputeNode):
    # skip cores 0 and 52 which are reserved for system procs
    cpu_ids = list(range(1,52)) + list(range(53,104))
    gpu_ids: List[IntStr]

    gpu_ids = []
    for gid in range(6):
        for tid in range(2):
            gpu_ids.append(str(gid) + "." + str(tid))

    @classmethod
    def get_job_nodelist(cls) -> List["AuroraNode"]:
        """
        Get all compute nodes allocated in the current job context
        """
        nodefile = os.environ["PBS_NODEFILE"]
        # a file containing a list of node hostnames, one per line
        # thetagpu01
        # thetagpu02
        with open(nodefile) as fp:
            data = fp.read()
        splitter = "," if "," in data else None
        hostnames = data.split(splitter)
        hostnames = [h.strip() for h in hostnames if h.strip()]
        node_ids: Union[List[str], List[int]]
        node_ids = hostnames[:]
        node_list = []
        for nid, hostname in zip(node_ids, hostnames):
            gpu_ids = cls.discover_gpu_list(hostname)
            assert isinstance(nid, str) or isinstance(nid, int)
            node_list.append(cls(nid, hostname, gpu_ids=gpu_ids))
        return node_list

    @classmethod
    def discover_gpu_list(cls, hostname: str) -> List[IntStr]:
        gpu_ids = cls.gpu_ids
        logger.info(f"{hostname} detected GPU IDs: {gpu_ids}")
        return gpu_ids

    @staticmethod
    def get_scheduler_id() -> Optional[int]:
        id = os.environ.get("PBS_JOBID")
        if id is not None:
            return int(id.split(".")[0])
        return None

    def assign(self, job_id: int, num_cpus: int = 0, num_gpus: int = 0, occupancy: float = 1.0) -> Dict[str, Any]:
        if job_id in self.jobs:
            raise ValueError(f"Already have job {job_id}")

        self.occupancy += occupancy
        if self.occupancy > 0.999:
            self.occupancy = 1.0

        # On an aurora node, gpus 0-2 should be assigned to cpus 1-51
        #                    gpus 3-5 should be assigned to cpus 53-103
        # Also, cpus should be assigned from the same socket
        idle_socket0_cpus = [cpu for cpu in self.idle_cpus if cpu < 52]
        idle_socket1_cpus = [cpu for cpu in self.idle_cpus if cpu > 52]
        idle_socket0_cpus.sort()
        idle_socket1_cpus.sort()

        assigned_gpus = self.idle_gpus[:num_gpus]
        if assigned_gpus:
            assigned_sockets = [0 if float(gpu) < 3 else 1 for gpu in assigned_gpus]
            num_cpus_per_gpu = num_cpus // num_gpus
            assigned_cpus = []
            for sock in assigned_sockets:
                if sock == 0:
                    assigned_cpus += idle_socket0_cpus[:num_cpus_per_gpu]
                    idle_socket0_cpus = [i for i in idle_socket0_cpus if i not in assigned_cpus]
                elif sock == 1:
                    assigned_cpus += idle_socket1_cpus[:num_cpus_per_gpu]
                    idle_socket1_cpus = [i for i in idle_socket1_cpus if i not in assigned_cpus]
            if len(assigned_cpus) < num_cpus:
                num_remainder = num_cpus - len(assigned_cpus)
                idle_both_sockets = idle_socket0_cpus + idle_socket1_cpus
                assigned_cpus += idle_both_sockets[:num_remainder]
        else:
            if len(idle_socket0_cpus) > num_cpus:
                assigned_cpus = idle_socket0_cpus[:num_cpus]
            elif len(idle_socket1_cpus) > num_cpus:
                assigned_cpus = idle_socket1_cpus[:num_cpus]
            else:
                assigned_cpus = self.idle_cpus[:num_cpus]

        if assigned_cpus:
            self.busy_cpus.extend(assigned_cpus)
            self.idle_cpus = [i for i in self.idle_cpus if i not in assigned_cpus]
        if assigned_gpus:
            self.busy_gpus.extend(assigned_gpus)
            self.idle_gpus = [i for i in self.idle_gpus if i not in assigned_gpus]
        resource_spec = {
            "cpu_ids": assigned_cpus,
            "gpu_ids": assigned_gpus,
            "occupancy": occupancy,
        }
        self.jobs[job_id] = resource_spec
        return resource_spec
