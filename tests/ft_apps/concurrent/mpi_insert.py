from mpi4py import MPI
import balsam.launcher.dag as dag

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

job_name = f"hello{rank}"
dag.add_job(name=job_name, workflow="test", application="hello", num_nodes=1,
            ranks_per_node=1)
print(f"Rank {rank} added job: success")
