from .app_run import AppRun


class MPICHRun(AppRun):
    """
    https://wiki.mpich.org/mpich/index.php/Using_the_Hydra_Process_Manager
    """

    launch_command = "mpiexec"

    def get_launch_args(self):
        env_args = [("--env", f'{var}="{val}"') for var, val in self.env.items()]
        nid_str = ",".join(map(str, self.node_ids))
        return [
            "-n",
            self.num_ranks,
            "--ppn",
            self.ranks_per_node,
            *[arg for pair in env_args for arg in pair],
            "--hosts",
            nid_str,
        ]
