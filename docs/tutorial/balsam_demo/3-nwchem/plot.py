#!/usr/bin/env python
# plot.py

'''Summarize results in plottable text format'''
from balsam.launcher.dag import current_job

results = [
    (task.data['r'], task.data['energy'])
    for task in current_job.get_parents()
]

results = sorted(results, key=lambda pair: pair[0])
print("%18s %18s" % ("O-H length / Ang", "Energy / a.u."))
for r,e in results:
    print("%18.3f %18.6f" % (r,e))
