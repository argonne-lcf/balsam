import sys
import time

if len(sys.argv) == 1:
    delay = 10
else:
    delay = int(sys.argv[1])

if 'parallel' in ' '.join(sys.argv):
    from mpi4py import MPI
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
else:
    rank = 0

print(f"Rank {rank} Sleeping for a long time...")
sys.stdout.flush()
time.sleep(delay)
if rank == 0: print("Done")
