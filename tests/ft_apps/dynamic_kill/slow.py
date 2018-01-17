from mpi4py import MPI
import time

rank = MPI.COMM_WORLD.Get_rank()
if rank == 0: print("Sleeping for a long time...")

time.sleep(20)

if rank == 0: print("Done")
