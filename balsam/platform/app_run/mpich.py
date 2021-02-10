from .app_run import SubprocessAppRun


class MPICHRun(SubprocessAppRun):
    """
    https://wiki.mpich.org/mpich/index.php/Using_the_Hydra_Process_Manager
    """

    def _build_cmdline(self) -> str:
        node_ids = [h for h in self._node_spec.hostnames]
        env_args = [("--env", f'{var}="{val}"') for var, val in self._envs.items()]
        nid_str = ",".join(map(str, node_ids))
        args = [
            "mpiexec",
            "-n",
            self.get_num_ranks(),
            "--ppn",
            self._ranks_per_node,
            *[arg for pair in env_args for arg in pair],
            "--hosts",
            nid_str,
            self._cmdline,
        ]
        return " ".join(str(arg) for arg in args)
