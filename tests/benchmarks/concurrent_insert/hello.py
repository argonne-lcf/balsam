from mpi4py import MPI
import balsam.launcher.dag as dag

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

count = dag.BalsamJob.objects.all().count()
print("Hello from {rank}, Jobs count is {count}")
