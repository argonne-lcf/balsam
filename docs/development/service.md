# The Service Modules

## Service module interface
Service Modules must be split into separate Python module files under the `balsam.service`
subpackage.  Each service module has a top-level `run()` function
in the module scope. This function must accept keyword arguments corresponding to the
service module configuration keys.

This configuration:
```yaml
service_modules:
    batch_job_manager:
        scheduler_interface: balsam.platform.scheduler.CobaltThetaScheduler
        poll_interval_sec: 30
        allowed_projects:
            - datascience
        default_project: datascience
        allowed_queues:
            -   name: default
                max_nodes: 4096
                max_walltime: 24h
                max_queued: 20
            -   name: debug_cache_quad
                max_nodes: 8
                max_walltime: 1h
                max_queued: 1
```

results in creation of a `multiprocessing.Process` instance like:

```py3
Process(
    target=batch_job_manager.run,
    kwargs=dict(
        scheduler_interface=SchedulerClass,
        poll_interval_sec=30,
        allowed_projects=allowed_projects_list,
        default_project="datascience",
        allowed_queues=allowed_queues_list
    )
)
```

## Balsam service startup process
* User invokes `balsam service start` in a Site directory
* Concurrent startups are protected by PID file
* CLI loads local & global config
* Sets up global logging config
* Gets list of service modules & their configuration from component factory
* Launches `module.run()` Process for each
* Polls status, restarts as necessary

## BatchJobManager

Interfaces to scheduler, performs job submission and syncs qstat state with Balsam API.
Does **not** decide when to submit more jobs -- that is the responsibility of `AutoScaler`

### BatchJob sync protocol:

1. `GET` list of currently active batchjobs (filter by Site, exclude inactive)
2. Perform local `qstat`
3. For each active `BatchJob`, compare with the qstat record and take action:
    * `pending_submission`: perform `qsub`, update `scheduler_id` and set `state="queued"`
        * on submission error, set `status_message=error` and `state="submit-failed"`
    * `pending_deletion` 
        * if in qstat list: qdel (leave it as "pending-deletion" until its actually gone)
        * if not in qstat list (i.e. job cleanly exited or it never ran), set `state=finished` 
          and parse logs to set `start_time` and `end_time`
    * On any status change, update `state`
    * If a job finished for any reason, parse the logs to update `start_time` and `end_time`
    * If any parameters like walltime were changed, run `qalter` if possible.  If it's too late; revert the change
4. Apply bulk-update via PATCH on list view
    * For any `running` job, add `revert=True` to the patch dict. This ensures that the running job parameters are consistent with the scheduler state

### Site sync 
* Periodically update Site nodelist, state of all job queues


## AutoScaler

Monitors Job backlog, currently queued BatchJobs, currently available resources.  Decides
when a new queue submission is necessary and posts a new `BatchJob` object to the API.
Modes:

* Fixed submissions (keep N jobs queued at all times)
* Backfill sniper
    * Count running, preprocessed, staged_in jobs
    * backlog = staged_in + preprocessed
    * Get all backfill windows
    * submit based on:
        * minimum job submission heuristic
        * available backfill windows
        * aggregate node requirement of runnable jobs
        * number of currently available nodes (from running BatchJobs) 
        * number of expected-soon-to-be-available nodes (recently queued in backfill)
    * deletes BatchJobs that did not go through within X minutes

## Transfer

* Workdir existence is ensured/double-checked before Stage-in, Preproc, Run
* Syncs with `TransferItems` in the API
* Posts transfer tasks to the Globus API and updates the `TransferItem.globus_transfer_id` in Balsam API
* Does not need to acquire Jobs: directly GET Transfers for `READY` Jobs at the current Site

Settings:

* stage in batch size
* max active transfers
* max submitted transfers
* poll interval
* max backlog: const or callable
    * heuristic: max(20, num_running)


## Processing

* Workdir existence is ensured/double-checked before Stage-in, Preproc, Run
* Runs pre- and post-processing scripts
* Multiprocessing pool with shared `JobSource` and `StatusUpdater`
* No need to acquire locks since there is no race
* Fetch staged_in, done, error, timeout jobs
