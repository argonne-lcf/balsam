---
hide:
  - toc
---

![logo](./img/balsam-black.png){ style="width: 55%; display: block; margin: 0 auto" }

*This page is for the Balsam 0.6 pre-release. Click [here for stable Balsam 0.5 docs.](https://balsam.readthedocs.io/en/master)*

A unified platform to manage high-throughput workflows across the HPC landscape.

**Run Balsam on any laptop, cluster, or supercomputer.**

```console
$ pip install --pre balsam-flow 
$ balsam login
$ balsam site init my-site
```

![site-init](./img/balsam-init.gif){ style="width: 80%; display: block; margin: 0 auto" }

**Declare HPC Apps and execution lifecycle hooks.**

```python
from balsam.api import ApplicationDefinition

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

* Easy `pip` installation [runs out-of-the-box on several HPC systems](user-guide/installation.md) and is [easily adaptable to others](./development/porting.md).
* [Balsam Sites](./user-guide/site-config.md) are remotely  controlled by design: submit and monitor workflows from *anywhere*
* [Run any existing application, with flexible execution environments and job lifecycle hooks](./user-guide/appdef.md)
* [High-throughput and fault-tolerant task execution](./user-guide/batchjob.md) on diverse resources
* Define data dependencies for any task: [Balsam orchestrates the necessary data transfers](./user-guide/transfer.md)
* [Elastic queueing](./user-guide/elastic.md): auto-scale resources to the workload size
* [Monitoring APIs](./user-guide/monitoring.md): query recent task failures, node utilization, or throughput


