import time
from sys import exit
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('number', type=int)
    parser.add_argument('--sleep', type=int, default=0)
    parser.add_argument('--retcode', type=int, default=0)
    args = parser.parse_args()

    print(args.number**2)
    if args.sleep:
        time.sleep(args.sleep)

    exit(args.retcode)
