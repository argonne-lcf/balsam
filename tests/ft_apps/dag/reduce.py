#!/Users/misha/anaconda3/envs/testmpi/bin/python
import sys

total = 0.0
for fname in sys.argv[1:]:
    area = float(open(fname).read())
    total += area

print("Total area:", total)
