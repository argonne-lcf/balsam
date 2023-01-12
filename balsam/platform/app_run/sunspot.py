import os

from .app_run import SubprocessAppRun


class SunspotRun(SubprocessAppRun):
    """
    https://www.open-mpi.org/doc/v3.0/man1/mpiexec.1.php
    """

    def _build_cmdline(self) -> str:
        node_ids = [h for h in self._node_spec.hostnames]
        cpu_bind = self._launch_params.get("cpu_bind", "none")
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
            "--envall",
            self._cmdline,
        ]
        return " ".join(str(arg) for arg in args)

    # Overide default because sunspot does not use CUDA
    def _set_envs(self) -> None:
        envs = os.environ.copy()
        envs.update(self._envs)
        envs["OMP_NUM_THREADS"] = str(self._threads_per_rank)

        # Check the assigned GPU ID list from the first compute node:
        gpu_ids = self._node_spec.gpu_ids[0]
        if gpu_ids:
            envs["ZE_ENABLE_PCI_ID_DEVICE_ORDER"] = "1"
            envs["ZE_AFFINITY_MASK"] = ",".join(map(str, gpu_ids))

        self._envs = envs
