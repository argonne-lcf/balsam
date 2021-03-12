from .app_run import SubprocessAppRun


class SummitJsrun(SubprocessAppRun):
    """
    https://docs.olcf.ornl.gov/systems/summit_user_guide.html#running-jobs
    """

    launch_command = "jsrun"
    cores_per_node = 42
    ranks_per_task = 1  # this forces 1 MPI rank to 1 resource task TODO: make this settable

    def _build_cmdline(self) -> str:
        args = [
            SummitJsrun.launch_command,
            "-n",
            self.get_num_ranks(),  # number of "resource sets"
            "-a",
            self.ranks_per_task,
            "-c",
            int(self.cores_per_node / self._ranks_per_node),  # number of cores per resource set
            "-g",
            self._gpus_per_rank,  # gpus per resource set
            self._cmdline,
        ]
        return " ".join(str(arg) for arg in args)
