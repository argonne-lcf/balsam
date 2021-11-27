# Scheduling Launchers

Unless [Auto Scaling](./elastic.md) is enabled, Jobs do not automatically run in
Balsam.  After the Site agent completes data staging and preprocessing for a
Job, it waits in the `PREPROCESSED` state until a launcher (pilot job) submitted
to the HPC batch scheduler takes over.

Launchers run independently of the Site agent, continuously fetching and 
executing runnable Jobs on the available compute resources.  They track
occupancy of the allocated CPUs/GPUs while launching Jobs with the requested
resources. Because launchers acquire Jobs on-the-fly, you can submit Jobs to any
system, and they will execute
*in real-time* on an existing allocation!

The Balsam launcher thus handles the compute-intensive core of the Job
lifecycle: from `PREPROCESSED` to `RUNNING` to `RUN_DONE`. As users, we need
only submit a `BatchJob` to any one of our Sites.  The `BatchJob` represents an
HPC resource allocation as a fixed block of node-hours.  Sites will
handle new `BatchJobs` by generating the script to run a launcher and submitting
it to the local HPC scheduler.

## Using the CLI

If we are *inside* a Site directory, we can submit a `BatchJob` to that Site
from the CLI:

```bash
$ balsam queue submit -q QUEUE -A PROJECT -n 128 -t 70 -j mpi 
```

The CLI works remotely, too: we just need to [target a specific `--site`](./cli.md) so Balsam knows where you want things to run:

```bash
$ balsam queue submit --site=my-site -q QUEUE -A PROJECT -n 128 -t 70 -j mpi 
```

Balsam will perform the appropriate submission to the underlying HPC scheduler
and synchronize your `BatchJobs` with the local scheduler state. Therefore, instead of checking queues locally (e.g. `qstat`), we can check on `BatchJobs` across all of our Sites with a simple command:

```bash
$ balsam queue ls --site=all
```

!!! note "Systems without a batch scheduler"
    Use `-n 1 -q local -A local` when creating `BatchJobs` at generic
    MacOS/Linux Sites without a real batch scheduler or multiple nodes. The OS
    process manager takes the place of the resource manager, but everything else
    looks the same!


## Selecting a Launcher Job Mode

All of the `queue submit` options pass through to the usual scheduler interface
(like `sbatch` or `qsub`), except for the `-j/--job-mode`  flag, which may be
either `mpi` or `serial`. These refer to the **launcher job modes**, which determines the pilot job implementation that will actually run.

- `mpi` mode is the most flexible and should be preferred unless you have a
particularly extreme throughput requirement or need to use one of the
workarounds offered by `serial` mode.  The `mpi` launcher runs on the head-node
of the `BatchJob` and executes each job using the system's MPI launch command
(e.g. `srun` or `mpirun`).
- `serial` mode only handles single-process (non-distributed memory) Jobs that
run within a single compute node.  Higher throughput of fine-grained tasks (e.g.
millions of single-core tasks) is achieved by running a worker process on *each
compute node* and fanning out cached `Jobs` acquired from the REST API.

Both launcher modes can simultaneously execute multiple applications per node,
as long as the underlying HPC system provides support.  This is not always the
case: for example, on ALCF's Theta-KNL system, `serial` mode is required to pack
multiple runs per node.

!!! note "You can submit multiple BatchJobs to a Site"
    Balsam launchers cooperatively divide and conquer the runnable Jobs at a
    Site.  You may therefore choose between queueing up *fewer large* BatchJobs
    or *several smaller* BatchJobs simultaneously.  On a busy HPC cluster,
    smaller BatchJobs can get through the queues faster and improve overall
    throughput.

## Using the API

A unique capability of the [Balsam Python API](./api.md) is that it allows us
programatically manage HPC resources (via `BatchJob`) and tasks (via `Job`) on
equal footing. We can submit and monitor `Jobs` and `BatchJobs` at any Site with
ease, using a single, consistent programming model.

```python
from balsam.api import Job, BatchJob
# Create Jobs:
job = Job.objects.create(
    site_name="myProject-theta-gpu",
    app_id="SimulationX",
    workdir="test-runs/foo/1",
)

# Or allocate resources:
BatchJob.objects.create(
    site_id=job.site_id,
    num_nodes=1,
    wall_time_min=20,
    job_mode="mpi",
    project="datascience",
    queue="full-node",
)
```

We can query `BatchJobs` to track how many resources are currently available or waiting in the queue at each Site:

```python
queued_nodes = sum(
    batch_job.num_nodes
    for batch_job in BatchJob.objects.filter(site_id=123, state="queued")
)
```

Or we can instruct Balsam to cleanly terminate an allocation:

```python
BatchJob.objects.filter(scheduler_id=1234).update(state="pending_deletion")
```

Jobs running in that `BatchJob` will be marked `RUN_TIMEOUT` and handled by
their respective Apps' `handle_timeout` hooks.

## Job Templates and specialized parameters
Behind the scenes, each `BatchJob` materializes as a batch job script rendered
from the Site's [job template](./site-config.md#customizing-the-job-template).
These templates can be customized to support new scheduler flags, load global
modules, or perform general pre-execution logic. These templates also accept
optional, system-specific parameters that can be passed on the CLI via `-x` or
to the BatchJob `optional_params` dictionary.


### Theta-KNL Optional Params
On Theta-KNL, we can prime the LDAP cache on each compute node prior to a large-scale ensemble of Singularity jobs.  This is necessary to avoid a system error that arises in Singularity startup at scale.

With the CLI:
```bash
$ balsam queue submit -x singularity_prime_cache=yes  # ..other args
```

With the Python API:
```python
BatchJob.objects.create(
    # ...other kwargs
    optional_params={"singularity_prime_cache": "yes"}
)
```
 
### ThetaGPU
On Theta-GPU, we can partition each of the 8 physical A100 GPUs into 2, 3, or 7
Multi-Instance GPU (MIG) resources.  This allows us to achieve higher GPU
utilization with high-throughput tasks consuming a fraction of the 40 GB device
memory.  Jobs using a MIG instance should still request a single logical GPU
with `gpus_per_rank=1` but specify a higher node-packing (e.g.
`node_packing_count` should be 8*3 = `24` for a 3-way MIG partitioning).

With the CLI:
```bash
$ balsam queue submit -x mig_count=3  # ..other args
```

With the Python API:
```python
BatchJob.objects.create(
    # ...other kwargs
    optional_params={"mig_count": "3"}
)
```

## Restricting BatchJobs with tags
We strongly encourage the use of [descriptive
tags](./jobs.md#tagging-jobs) to facilitate monitoring Jobs.
Another major use of tags is to *restrict* which Jobs can run in a given BatchJob.

The default behavior is that a BatchJob will run **as many Jobs as possible**:
Balsam decides what runs in any given allocation. But perhaps we wish to prioritize a certain group of runs, or deliberately run Jobs in separate partitions as part of a scalability study.  

This is easy with the CLI:

```bash
# Only run jobs with tags system=H2O and scale=4
$ balsam queue submit -n 4 --tag system=H2O --tag scale=4  # ...other args
```

Or with the API:
```python
BatchJob.objects.create(
    # ...other kwargs
    num_nodes=4,
    filter_tags={"system": "H2O", "scale": "4"}
)
```

## Partitioning BatchJobs

By default, a single launcher process manages the entire allocation of compute nodes with a single job mode of either `mpi` or `serial`.  In advanced use-cases, we can actually divide a single queue submission/allocation into *multiple* launcher partitions.

Each of this partitions can have its own number of compute nodes, job mode, and filter tags.  This can be useful in different contexts: 

- Dividing an allocation to run a mixed workload of MPI applications and high-throughput sub-node applications.  
- Improving scalability to large node counts by parallelizing the job launcher.

We can request that a BatchJob is split into partitions on the CLI. In this
example, we split a 128-node allocation into a 2-node MPI launcher (to run some
"leader" MPI app on 2 nodes), while the remaining 126 nodes are managed by the
efficient `serial` mode launcher for high-throughput.

```bash
$ balsam queue submit -n 128 -p mpi:2 -p serial:126 # ...other args
```

We could also apply tag restrictions to ensure that the right Jobs run in the
right partition:

```bash
$ balsam queue submit -n 128 -p mpi:2:role=leader -p serial:126:role=worker # ...other args
```

With the Python API, this looks like:
```python
BatchJob.objects.create(
    # ...other kwargs
    num_nodes=128,
    partitions=[
        {"job_mode": "mpi", "num_nodes": 2, "filter_tags": {"role": "leader"}},
        {"job_mode": "serial", "num_nodes": 126, "filter_tags": {"role": "worker"}},
    ]
)
```