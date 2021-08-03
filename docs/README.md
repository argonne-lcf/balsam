![logo](./img/balsam-black.png)

*This page is for the Balsam 0.6 pre-release. Click [here for stable Balsam 0.5 docs.](https://balsam.readthedocs.io/en/master)*

A unified platform to manage high-throughput workflows across the HPC landscape.

**Run Balsam Sites on any laptop, cluster, or supercomputer.**

```console
$ pip install --pre balsam-flow 
$ balsam login
$ balsam site init my-site
```

**Declare HPC Apps and execution lifecycle hooks.**

```python
# my-site/apps/demo.py
from balsam.site import ApplicationDefinition

class Hello(ApplicationDefinition):
    command_template = "echo hello {{ name }}"

    def preprocess(self):
        print("Preprocessing!")
        self.job.state = "PREPROCESSED"
```

**Run Apps from anywhere, using the unified Balsam service.**

```python
# On any other machine with internet access...
from balsam.api import Job, BatchJob

# Submit 10 demo.Hello Jobs to run at my-site
jobs = [
    Job(
        site_path="my-site",
        app_name="demo.Hello",
        workdir=f"test/{n}",
        parameters={"name": f"world {n} out of 10!"},
    )
    for n in range(10)
]
Job.objects.bulk_create(jobs)

# Request 1 compute node at my-site for 10 minutes
BatchJob.objects.create(
    site_id=jobs[0].site_id,
    num_nodes=1,
    wall_time_min=10,
    job_mode="serial",
    project="datascience",
    queue="debug-cache-quad",
)

```

## Features

* Simple `pip` installation on any machine with internet access
* Distributed by default: submit and monitor workflows from *anywhere*
* Run any existing application, with flexible execution environments and job lifecycle hooks
* Define data dependencies for any task: Balsam orchestrates the necessary data transfers
* High-throughput and fault-tolerant task execution on diverse resources
* Elastic queueing: auto-scale resources to the workload size
* Monitoring APIs: query recent task failures, node utilization, or throughput
* Portable: [easy to install on several HPC systems](user-guide/installation.md) and easily adaptable to others.


