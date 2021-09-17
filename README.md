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
from balsam.api import ApplicationDefinition

class Hello(ApplicationDefinition):
    site = "my-site"
    command_template = "echo hello {{ name }}"

    def handle_timeout(self):
        self.job.state = "RESTART_READY"
```

**Run Apps from anywhere, thanks to the unified Balsam service.**

```python
# On any machine with internet access...
from balsam.api import Job, BatchJob

# Create Jobs:
job = Job.objects.create(
    site_name="my-site",
    app_id="Hello",
    workdir="test/say-hello",
    parameters={"name": "world!"},
)

# Or allocate resources:
BatchJob.objects.create(
    site_id=job.site_id,
    num_nodes=1,
    wall_time_min=10,
    job_mode="serial",
    project="local",
    queue="local",
)
```

**Define and run Python Apps on heterogeneous resources, from a single session.**

```python
import numpy as np

class MyApp(ApplicationDefinition):
    site = "theta-gpu"

    def run(self, vec):
        from mpi4py import MPI
        rank = MPI.COMM_WORLD.Get_rank()
        print("Hello from rank", rank)
        return np.linalg.norm(vec)

jobs = [
    MyApp.submit(
        workdir=f"test/{i}", 
        vec=np.random.rand(3), 
        ranks_per_node=4,
        gpus_per_rank=0,
    )
    for i in range(10)
]

for job in Job.objects.as_completed(jobs):
   print(job.workdir, job.result())
```



## Features

* Easy `pip` installation [runs out-of-the-box on several HPC systems](user-guide/installation.md) and is [easily adaptable to others](./development/porting.md).
* [Balsam Sites](./user-guide/site-config.md) are remotely  controlled by design: submit and monitor workflows from *anywhere*
* [Run any existing application, with flexible execution environments and job lifecycle hooks](./user-guide/appdef.md)
* [High-throughput and fault-tolerant task execution](./user-guide/batchjob.md) on diverse resources
* Define data dependencies for any task: [Balsam orchestrates the necessary data transfers](./user-guide/transfer.md)
* [Elastic queueing](./user-guide/elastic.md): auto-scale resources to the workload size
* [Monitoring APIs](./user-guide/monitoring.md): query recent task failures, node utilization, or throughput


