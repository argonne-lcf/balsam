#!/usr/bin/env python
import os
import sys
from balsam.core.models import BalsamJob, ApplicationDefinition
from balsam.launcher.dag import current_job

# Bootstrap app if it's not already in the DB
ApplicationDefinition.objects.get_or_create(
    name = 'square',
    executable = os.path.abspath(__file__),
)

def run(job):
    """If we're inside a Balsam task, do the calculation"""
    x = job.data['x']
    y = x**2
    job.data['y'] = y
    job.save()

def create_jobs(N):
    """If we're on a command line, create N tasks to square a number"""
    for i in range(N):
        job = BalsamJob(
            name = f"square{i}",
            workflow = "demo-square",
            application = "square",
            node_packing_count=64, # Run up to 64 per node
        )
        job.data["x"] = i
        job.save()
    print(f"Created {N} jobs")

if __name__ == "__main__":
    if current_job:
        run(current_job)
    else:
        N = int(sys.argv[1])
        create_jobs(N)
