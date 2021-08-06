![logo](./img/balsam-black.png)

*This page is for the Balsam 0.6 pre-release. Click [here for stable Balsam 0.5 docs.](https://balsam.readthedocs.io/en/master)*

A unified platform to manage high-throughput workflows across the HPC landscape.

**Run Balsam on any laptop, cluster, or supercomputer.**

```console
$ pip install --pre balsam-flow 
$ balsam login
$ balsam site init my-site
```

![site-init](./img/balsam-init.gif)

**Declare HPC Apps and execution lifecycle hooks.**

```python
from balsam.site import ApplicationDefinition

class Hello(ApplicationDefinition):
    command_template = "echo hello {{ name }}"

    def handle_timeout(self):
        self.job.state = "RESTART_READY"
```

**Run Apps from anywhere, using the unified Balsam service.**

```python
# On any machine with internet access...
from balsam.api import Job, BatchJob

# Create Jobs:
job = Job.objects.create(
    site_path="my-site",
    app_name="demo.Hello",
    workdir="test/say-hello",
    parameters={"name": "world!"},
)

# Or allocate resources:
BatchJob.objects.create(
    site_id=job.site_id,
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


