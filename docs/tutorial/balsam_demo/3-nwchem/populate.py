#!/usr/bin/env python
# populate.py
'''Add Water potential energy scan tasks to DB'''

from balsam.launcher.dag import BalsamJob, add_dependency
import numpy as np

r_grid = np.linspace(0.8, 1.3)

water_scan = []
for i, r in enumerate(r_grid):
    job = BalsamJob(
        name=f"task{i}",
        workflow="demo",
        description=f"r = {r:.3f}",
        application = "nwchem-water",
        args = "input.nw",
        num_nodes = 1,
        ranks_per_node = 64,
        cpu_affinity = "depth",
        data = {'r':r, 'theta': 104.5},
    )
    water_scan.append(job)
    job.save()

plotjob = BalsamJob(
    name="plot",
    application="plot-pes",
    workflow="demo",
    input_files="",
)
plotjob.save()
for job in water_scan:
    add_dependency(parent=job, child=plotjob)

