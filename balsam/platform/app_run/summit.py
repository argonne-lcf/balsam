from .app_run import AppRun


class SummitJsrun(AppRun):
    """
    https://docs.olcf.ornl.gov/systems/summit_user_guide.html#running-jobs
    """

    launch_command = "jsrun"
    cores_per_node = 42

    def _build_cmdline(self) -> str:
        # nid_str = ",".join(map(str, self.node_ids))
        env_args = [("-E", f'{var}="{val}"') for var, val in self._envs.items()]
        args = [
            "-n",
            self.get_num_ranks(),  # number of "resource sets"
            "-a",
            1,  # this forces 1 MPI rank to 1 resource task TODO: make this settable
            "-c",
            int(self.cores_per_node / self._ranks_per_node),  # number of cores per resource set
            "-g",
            self._gpus_per_rank,  # gpus per resource set
            *[arg for pair in env_args for arg in pair],
        ]
        return " ".join(str(arg) for arg in args)
