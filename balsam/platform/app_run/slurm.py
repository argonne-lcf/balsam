import time

from .app_run import SubprocessAppRun


class SlurmRun(SubprocessAppRun):
    """
    https://slurm.schedmd.com/srun.html
    """

    def _build_cmdline(self) -> str:
        node_ids = [h for h in self._node_spec.hostnames]
        num_nodes = str(len(node_ids))
        args = [
            "srun",
            "-n",
            self.get_num_ranks(),
            "--ntasks-per-node",
            self._ranks_per_node,
            "--nodelist",
            ",".join(node_ids),
            "--nodes",
            num_nodes,
            "--cpus-per-task",
            self.get_cpus_per_rank(),
            self._cmdline,
        ]
        return " ".join(str(arg) for arg in args)

    def _pre_popen(self) -> None:
        time.sleep(0.01)
