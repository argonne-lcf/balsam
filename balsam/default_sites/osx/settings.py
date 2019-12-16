from balsam.platform import (mpirun, compute_node)
from balsam.client import ClientAPI
from pathlib import Path

here = Path(__file__).resolve().parent

class MPIRun(mpirun.OpenMPIRun):
    class DefaultOptions:
        num_nodes = 1
        ranks_per_node = 1
        threads_per_rank = 1
        threads_per_core = 4

MPIRUN = mpirun.OpenMPIRun()
MPIRUN.set_default_args(
    num_nodes = 1,
    ranks_per_node = 1,
)

CLIENT = ClientAPI.from_yaml(here)

SCHEDULER = LocalScheduler(
    num_cores=4
)

NODE_SPEC = compute_node.Theta
