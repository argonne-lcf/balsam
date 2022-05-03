from .app_run import SubprocessAppRun


class PolarisRun(SubprocessAppRun):
    """
    https://www.open-mpi.org/doc/v3.0/man1/mpiexec.1.php
    """

    def _build_cmdline(self) -> str:
        node_ids = [h for h in self._node_spec.hostnames]
        env_args = ",".join(self._envs.keys())
        nid_str = ",".join(map(str, node_ids))
        args = [
            "mpiexec",
            "-n",
            self.get_num_ranks(),
            #"--map-by",
            #f"ppr:{self._ranks_per_node}:node",
            "-envlist",
            env_args,
            "-hostlist",
            nid_str,
            self._cmdline,
        ]
        return " ".join(str(arg) for arg in args)
