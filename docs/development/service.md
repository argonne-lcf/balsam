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
            - default:
                max_nodes: 4096
                max_walltime: 24h
                max_queued: 20
            - debug_cache_quad:
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

## AutoScaler

Monitors Job backlog, currently queued BatchJobs, currently available resources.  Decides
when a new queue submission is necessary and posts a new `BatchJob` object to the API.

## Transfer

* Syncs with `TransferItems` in the API
* Posts transfer tasks to the Globus API and updates the `TransferItem.globus_transfer_id` in Balsam API


## Processing

* Runs pre- and post-processing scripts
* Multiprocessing pool with shared `StatusUpdater`
