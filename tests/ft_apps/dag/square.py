#!/Users/misha/anaconda3/envs/testmpi/bin/python
import argparse
import time
import sys

print("Hello from square")
parser = argparse.ArgumentParser()
parser.add_argument('infile')
parser.add_argument('--sleep', type=float, default=0)
parser.add_argument('--retcode', type=int, default=0)
args = parser.parse_args()

side_length = float(open(args.infile).read())
with open('square.dat', 'w') as fp:
    square = side_length**2
    fp.write(str(square) + "\n")

time.sleep(args.sleep)
sys.exit(args.retcode)
