import logging
import os

from balsam.platform.compute_node import SophiaNode

from .app_run import SubprocessAppRun

logger = logging.getLogger(__name__)


class SophiaRun(SubprocessAppRun):
    """
    https://www.open-mpi.org/doc/v3.0/man1/mpiexec.1.php
    """

    def _build_cmdline(self) -> str:
        node_ids = [h for h in self._node_spec.hostnames]

        # If the user does not set a cpu_bind option, set it to none
        if "cpu_bind" in self._launch_params.keys():
            cpu_bind = self._launch_params.get("cpu_bind")
        elif "--cpu-bind" in self._launch_params.keys():
            cpu_bind = self._launch_params.get("--cpu-bind")
        else:
            cpu_bind = "none"

        launch_params = []
        for k in self._launch_params.keys():
            if k != "cpu_bind" and k != "--cpu-bind":
                launch_params.append(str(self._launch_params[k]))

        nid_str = ",".join(map(str, node_ids))
        args = [
            "mpirun",
            "-n",
            self.get_num_ranks(),
            "--npernode",
            self._ranks_per_node,
            "--host",
            nid_str,
            "--bind-to",
            cpu_bind,
            *launch_params,
            self._cmdline,
        ]
        return " ".join(str(arg) for arg in args)

    def _set_envs(self) -> None:
        envs = os.environ.copy()
        envs.update(self._envs)

        # Here we grab the gpus assigned to the job from NodeSpec.  NodeSpec only
        # sets this for single node jobs.  For multinode jobs, gpu_ids below will
        # be an empty list of lists (e.g. [[], []]).
        gpu_ids = self._node_spec.gpu_ids[0]
        cpu_ids = self._node_spec.cpu_ids[0]
        logger.info(f"Sophia set_envs: gpu_ids={gpu_ids} cpu_ids={cpu_ids}")

        # Here we set CUDA_VISIBLE_DEVICES for single node jobs only.  We assume
        # for multinode jobs that the job has access to all gpus, and
        # CUDA_VISIBLE_DEVICES is set by the user, for example by local rank with an
        # gpu_affinity.sh script that wraps around the user application in the
        # ApplicationDefinition.
        # One special case: if your job has one node, 2 ranks, and 1 gpu per rank, the
        # code here will set CUDA_VISIBLE_DEVICES to "0,1" or "2,3" etc.  A user provided
        # gpu_affinity.sh script should take this assigment and use it to reset
        # CUDA_VISIBLE_DEVICES for each local rank.  The user script should NOT
        # round-robin the setting CUDA_VISIBLE_DEVICES for all 8 devices.
        if gpu_ids:
            envs["CUDA_DEVICE_ORDER"] = "PCI_BUS_ID"
            envs["CUDA_VISIBLE_DEVICES"] = ",".join(map(str, gpu_ids))
        envs["OMP_NUM_THREADS"] = str(self._threads_per_rank)
        self._envs = envs
