from .mpirun import MPIRun
class OpenMPIRun(MPIRun):
    """
    https://www.open-mpi.org/doc/v3.0/man1/mpiexec.1.php
    """
    launch_command = 'mpiexec'

    def get_launch_args(self):
        env_args = [ ('-x', var) for var in self.env.keys() ]
        nid_str = ",".join(map(str, self.node_ids))
        return [
            '-n', self.num_ranks,
            '--map-by', f'ppr:{self.ranks_per_node}:node',
            *[arg for pair in env_args for arg in pair],
            '-H', nid_str,
        ]

