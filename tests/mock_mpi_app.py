from mpi4py import MPI
import argparse
import time
from sys import exit

rank = MPI.COMM_WORLD.Get_rank()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--sleep', type=int, default=0)
    parser.add_argument('--retcode', type=int, default=0)
    args = parser.parse_args()

    print("Rank", rank, "on", MPI.Get_processor_name())
    if args.sleep:
        time.sleep(args.sleep)

    exit(args.retcode)
