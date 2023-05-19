import logging
import os

from .app_run import SubprocessAppRun

logger = logging.getLogger(__name__)


class PolarisRun(SubprocessAppRun):
    """
    https://www.open-mpi.org/doc/v3.0/man1/mpiexec.1.php
    """

    def _build_cmdline(self) -> str:
        node_ids = [h for h in self._node_spec.hostnames]

        cpu_bind = self._launch_params.get("cpu_bind", "none")
        if cpu_bind == "none" and self._gpus_per_rank > 0:
            gpu_device = self._envs["CUDA_VISIBLE_DEVICES"]
            gpu_ids = gpu_device.split(",")
            cpu_ids = self._node_spec.cpu_ids[0]

            cpu_bind_list = ["verbose,list"]
            for gid in gpu_ids:
                start_cpu = 32 - int(gid) * 8 - self.get_cpus_per_rank()
                cpu_bind_list.append(":")
                for icpu in range(self.get_cpus_per_rank()):
                    if icpu > 0:
                        cpu_bind_list.append(",")
                    cpu_bind_list.append(str(start_cpu + icpu))

            # start_cpu = 32 - 8 * (1 + gpu_device)
            # for i in range(8):
            #     cpu_bind_list.append(":" + str(start_cpu + i))
            cpu_bind = "".join(cpu_bind_list)
            logger.info(f"Polaris app_run: cpu_bind={cpu_bind} cpu_ids={cpu_ids} gpu_ids={gpu_ids}")

        launch_params = []
        for k in self._launch_params.keys():
            if k != "cpu_bind":
                launch_params.append("--" + k)
                launch_params.append(str(self._launch_params[k]))

        nid_str = ",".join(map(str, node_ids))
        args = [
            "mpiexec",
            "-np",
            self.get_num_ranks(),
            "-ppn",
            self._ranks_per_node,
            "--hosts",
            nid_str,
            "--cpu-bind",
            cpu_bind,
            "-d",
            self._threads_per_rank,
            *launch_params,
            self._cmdline,
        ]
        return " ".join(str(arg) for arg in args)

    # Overide default because sunspot does not use CUDA
    def _set_envs(self) -> None:

        envs = os.environ.copy()
        envs.update(self._envs)
        # Check the assigned GPU ID list from the first compute node:
        gpu_ids = self._node_spec.gpu_ids[0]

        if gpu_ids:
            envs["CUDA_DEVICE_ORDER"] = "PCI_BUS_ID"
            envs["CUDA_VISIBLE_DEVICES"] = ",".join(map(str, gpu_ids))
        envs["OMP_NUM_THREADS"] = str(self._threads_per_rank)
        self._envs = envs
