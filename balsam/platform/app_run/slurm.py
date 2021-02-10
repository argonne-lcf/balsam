from .app_run import AppRun


class SlurmRun(AppRun):
    """
    https://slurm.schedmd.com/srun.html
    """

    launch_command = "srun"

    def _build_cmdline(self) -> str:
        node_ids = [h for h in self._node_spec.node_ids]
        nid_str = ",".join(map(str, node_ids))
        num_nodes = str(len(node_ids))
        args = [
            "-n",
            self.get_num_ranks(),
            "--ntasks-per-node",
            self._ranks_per_node,
            "--nodelist",
            nid_str,
            "--nodes",
            num_nodes,
        ]
        return " ".join(str(arg) for arg in args)
