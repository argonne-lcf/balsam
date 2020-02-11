from .mpirun import MPIRun


class SlurmRun(MPIRun):
    """
    https://slurm.schedmd.com/srun.html
    """

    launch_command = "srun"

    def get_launch_args(self):
        nid_str = ",".join(map(str, self.node_ids))
        num_nodes = str(len(self.node_ids))
        return [
            "-n",
            self.num_ranks,
            "--ntasks-per-node",
            self.ranks_per_node,
            "--nodelist",
            nid_str,
            "--nodes",
            num_nodes,
        ]
