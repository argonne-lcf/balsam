#!/usr/bin/env python
# post.py
'''Parse Energy from NWChem output'''

from balsam.launcher.dag import current_job

outfile = current_job.name + ".out"
energy = None

with open(outfile) as fp:
    for line in fp:
        if 'Total SCF energy' in line:
            energy = float(line.split()[-1])
            break

current_job.data['energy'] = energy
current_job.save()

