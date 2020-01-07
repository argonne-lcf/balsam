from .mpirun import MPIRun
class ThetaAprun(MPIRun):
    """
    https://www.alcf.anl.gov/support-center/theta/running-jobs-and-submission-scripts
    """
    launch_command = 'aprun'

    def get_launch_args(self):
        nid_str = ",".join(map(str, self.node_ids))
        env_args = [ ('-e', f'{var}="{val}"') for var,val in self.env.items() ]
        return [
            '-n', self.num_ranks,
            '-N', self.ranks_per_node,
            *[arg for pair in env_args for arg in pair],
            '-L', nid_str,
            '-cc', self.cpu_affinity,
            '-d', self.threads_per_rank,
            '-j', self.threads_per_core,
        ]
