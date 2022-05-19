# Auto-scaling Resources with Balsam Elastic Queue

A Balsam Site does not automatically use your computing allocation by default. 
Instead, we must use the [BatchJob API or `balsam queue` CLI](./batchjob.md) to
explicitly request compute nodes at a given Balsam Site.  This follows the
principle of least surprise: as the User, you explicitly decide when and how
resources are spent. This is not a limitation since we can remotely submit
`BatchJobs` to any Site from a computer where Balsam is installed!

However, we can *opt-in* to enable the `elastic_queue` plugin that creates
`BatchJob` submissions on our behalf.  This can be a useful service
in bursty or *real-time* workloads: instead of micro-managing the queues, we
simply submit a stream of `Jobs` and allow the Site to provision resources
as needed over time.

## Enabling the Elastic Queue Plugin

Auto-scaling is enabled at a Site by setting the `elastic_queue` configuration appropriately inside of the `settings.yml` file.

You should find this line uncommented by default:

```yaml
elastic_queue: null
```

Following this line is a commented-block showing an example `elastic_queue` configuration.  We want to *comment out* the `elastic_queue: null` line and *uncomment* the configuration; setting each of the parameters appropriately for our use case.

Once the Plugin has been configured properly (see below), we must restart the Balsam Site Agent to load it:

```bash
$ balsam site sync

# Or if the Site isn't already running:
$ balsam site start
```

## Disabling the Elastic Queue Plugin

To disable, *comment out (or delete)* the current `elastic_queue` 
configuration in `settings.yml` and replace it with the line:

```yaml
elastic_queue: null
```

Then restart the Balsam Site to run without the elastic queue plugin:

```bash
$ balsam site sync
```

## Configuring the Elastic Queue

The configuration is fairly flexible to enable a wide range of use cases. This section explains the YAML configuration in chunks.

### Project, Queue, and Submit Frequency
Firstly, `service_period` controls the waiting period (in seconds) between
cycles in which a new `BatchJob` might be submitted to the queue.  The
`submit_project`, `submit_queue`, and `job_mode` are directly passed through to
the new `BatchJob`.  

The `max_queue_wait_time_min` determines how long a submitted `BatchJob` should
be enqueued before the elastic queue deletes it and tries re-submitting.  When
using backfill to grab idle nodes (see next section), it makes sense to set a
relatively short waiting time of 5-10 minutes.  Otherwise, this duration should
be increased to a reasonable upper threshold to avoid deleting `BatchJobs` that
have accrued priority in the queues.

The elastic queue will maintain up to `max_queued_jobs` in the queue at any
given time. This should be set to the maximum desired (or allowed) number of
simultaneously queued/running `BatchJobs` at the Site. 

```yaml hl_lines="2-5 12-13"
elastic_queue:
     service_period: 60
     submit_project: "datascience"
     submit_queue: "balsam"
     job_mode: "mpi"
     use_backfill: True
     min_wall_time_min: 35
     max_wall_time_min: 360
     wall_time_pad_min: 5
     min_num_nodes:  20
     max_num_nodes: 127
     max_queue_wait_time_min: 10
     max_queued_jobs: 20
```

### Wall Time and Backfilling

Many HPC systems use *backfilling* schedulers, which attempt to place small Jobs
while draining nodes for larger Jobs to start up at a determined future time.
By *opportunistically* sizing jobs to fit into these idle node-hour windows,
Balsam effectively "fills the gaps" in unused resources.  We enable this dynamic
sizing with `use_backfill: True`.

The interpretation of `min_wall_time_min` and `max_wall_time_min` depends on whether or not `use_backfill` is enabled:  

- **When `use_backfill` is `False`:** `min_wall_time_min` is ignored and
BatchJobs are submitted for a constant wallclock time limit of
`max_wall_time_min`.
- **When `use_backfill` is `True`:** Balsam selects backfill windows that are *at least* as long as `min_wall_time_min` (this is to avoid futile 5 minute submissions when all Jobs take at least 30 minutes). The wallclock time limit is then *the lesser of* the scheduler's backfill duration and `max_wall_time_min`.
- Finally, a "padding" value of `wall_time_pad_min` is subtracted from the 
  final wallclock time in all `BatchJob` submissions.  This should be set to a couple minutes when `use_backfill` is `True` and `0` otherwise.

```yaml hl_lines="6-9"
elastic_queue:
     service_period: 60
     submit_project: "datascience"
     submit_queue: "balsam"
     job_mode: "mpi"
     use_backfill: True
     min_wall_time_min: 35
     max_wall_time_min: 360
     wall_time_pad_min: 5
     min_num_nodes:  20
     max_num_nodes: 127
     max_queue_wait_time_min: 10
     max_queued_jobs: 20
```

### Node Count

Finally, the `min_num_nodes` and `max_num_nodes` determine the permissible range
of node counts in submitted `BatchJobs`.
When operating with the `use_backfill=True` constraint, backfill windows
smaller than `min_num_nodes` will be ignored.  Otherwise, BatchJob submissions use `min_num_nodes` as a lower bound. Likewise, `max_num_nodes` gives an upper bound on the BatchJob's node count. 

The **actual** submitted BatchJob node count falls somewhere in this range.  It is determined from the *difference* between how many nodes are currently requested (queued or running BatchJobs) and the aggregate *node footprint* of all runnable Jobs.

```yaml hl_lines="10-11"
elastic_queue:
     service_period: 60
     submit_project: "datascience"
     submit_queue: "balsam"
     job_mode: "mpi"
     use_backfill: True
     min_wall_time_min: 35
     max_wall_time_min: 360
     wall_time_pad_min: 5
     min_num_nodes:  20
     max_num_nodes: 127
     max_queue_wait_time_min: 10
     max_queued_jobs: 20
```

Therefore, the elastic queue automatically controls the **size** and **number**
of requested BatchJobs as the workload grows.  We can think of each `BatchJob`
as a *flexibly-sized block* of resources, and the elastic queue creates multiple
blocks (one per `service_period`) while choosing their sizes.   If one BatchJob
does not accommodate the incoming volume of tasks, then multiple BatchJobs of the
maximum size are submitted at each iteration.

When the incoming Jobs slow down and the backlog falls inside the
`(min_num_nodes, max_num_nodes)` range, the `BatchJobs` reduce down to a single,
*smaller* allocation of resources.  As utilization decreases and launchers
become idle, the nodes are released according to the launcher's `idle_ttl_sec`
configuration (also in `settings.yml`).
