import os

from .app_run import SubprocessAppRun


class ThetaGPURun(SubprocessAppRun):
    """
    https://www.open-mpi.org/doc/v3.0/man1/mpiexec.1.php
    """

    def _build_cmdline(self) -> str:
        node_ids = [h for h in self._node_spec.hostnames]
        env_args = [("-x", var) for var in self._envs.keys()]
        nid_str = ",".join(map(str, node_ids))
        hostfile = os.environ["COBALT_NODEFILE"]
        args = [
            "mpirun",
            "-hostfile",
            hostfile,
            "--oversubscribe",
            "--bind-to none",
            "-n",
            self.get_num_ranks(),
            "-npernode",
            str(self._ranks_per_node),
            *[arg for pair in env_args for arg in pair],
            "--host",
            nid_str,
            self._cmdline,
        ]
        return " ".join(str(arg) for arg in args)
