#!/Users/misha/anaconda3/envs/testmpi/bin/python
import os
import random
import argparse
import time
import sys

parser = argparse.ArgumentParser()
parser.add_argument('--sleep', type=float, default=0)
parser.add_argument('--retcode', type=int, default=0)
args = parser.parse_args()

time.sleep(args.sleep)

num_sides = int(os.environ['BALSAM_FT_NUM_SIDES'])
num_ranks = int(os.environ['BALSAM_FT_NUM_RANKS'])

if num_ranks > 1:
    from mpi4py import MPI
    COMM = MPI.COMM_WORLD
    rank = COMM.Get_rank()
    print(f"Rank {rank}")
else:
    print("Rank 0")
    rank = 0

if rank == 0:
    for i in range(num_sides):
        side_length = random.uniform(0.5,5)
        with open(f"side{i}.dat", 'w') as fp:
            fp.write(str(side_length) + "\n")

sys.exit(args.retcode)
