![logo](./img/small3.png)

*This page is for the Balsam 0.6 pre-release. Click [here for stable Balsam 0.5 docs.](https://balsam.readthedocs.io/en/master)*

A unified platform to manage high-throughput workflows across the HPC landscape.

Create Balsam Sites on any laptop, cluster, or supercomputer. 
The Balsam service provides a central API for you to submit tasks to these sites from anywhere.

```python
from balsam.api import App, Job, BatchJob

# Add 10 fibonacci tasks
fibo_app = App.objects.get(class_path="demo.fibonacci")
jobs = [
    Job(
        app_id=fibo_app.id,
        workdir=f"demo-sweep/{n}",
        parameters={"N": n},
    )
    for n in range(10)
]
Job.objects.bulk_create(jobs)

# Request 2 nodes for 10 minutes at the corresponding Site
BatchJob.objects.create(
    site_id=fibo_app.site_id,
    num_nodes=2,
    wall_time_min=10,
    job_mode="serial",
    project="datascience",
    queue="debug-cache-quad",
)

```

## Features

* Simple `pip` installation on any machine with internet access
* Control apps as Python classes: flexible environments and lifecycle hooks
* Distributed by default: submit and monitor tasks securely from *anywhere*
* Define data dependencies for any task: Balsam orchestrates the necessary data transfers
* High-throughput and fault-tolerant task execution on diverse resources
* Elastic queueing: auto-scale resources to the workload size
* Monitoring APIs: query recent task failures, node utilization, or throughput
* Portable: [easy to install on several HPC systems](user-guide/installation.md) and easily adaptable to others.

## Quick Start

Add Balsam to any Python environment with `pip`:

```bash
# Use --pre to get the Balsam0.6 pre-release
$ pip install --pre balsam-flow 
```

Login to the Balsam API service:
```bash
$ balsam login
```

Now register your first Balsam site:
```bash
$ balsam site init my-site
cd my-site
balsam site start
```

This creates a directory `my-site/` serving as a project space in which you can define applications and manage
workflows remotely.



## To view the docs in your browser:

Navigate to top-level balsam directory (where `mkdocs.yml` is located) and run:
```
mkdocs serve
```

Follow the link to the documentation. Docs are markdown files in the `balsam/docs` subdirectory and can be edited 
on-the-fly.  The changes will auto-refresh in the browser window.
