# Project Layout

This pages gives an overview of the Balsam subpackages, important components therein, and their relationships to each other.

## `balsam.server`

This subpackage is the fully self-contained codebase for the API server, implemented with [FastAPI](https://fastapi.tiangolo.com/) and [SQLAlchemy](https://docs.sqlalchemy.org/en/13/).

- `server.main` defines the top-level URL routes into views located in `balsam.server.routers`
- `balsam.server.routers` defines the possible API actions
- `balsam.server.models` encapsulates the database and any actions that involve database communication.

## `balsam.client`

This package defines the low-level `RESTClient` interface used by all the Balsam Python clients.  
The implementations capture the details of authentication to the Balsam API and performing HTTP requests.

## `balsam._api`

Whereas the `RESTClient` provides a lower-level interface (exchanging JSON data over HTTP),
the `balsam._api` defines Django ORM-like `Models` and `Managers` to emulate the original Balsam API:

```python3
from balsam.api import Job

finished_jobs = Job.objects.filter(state="JOB_FINISHED")
```

This is the primary user-facing API.  Under the hood, it uses a `RESTClient` implementation  from the `balsam.client` subpackage to actually make HTTP requests.

## `balsam.schemas`

[Pydantic](https://pydantic-docs.helpmanual.io/) is used to define the API data structures and perform validation. The schemas under `balsam.schemas` are used _both_ by the user-facing `balsam._api` classes and the backend `balsam.server.routers` API. Thus when an update to the schema is made, both the client and server-side code inherit the change.

## `balsam.platform`

The `platform` subpackage contains all the **platform-specific** interfaces to various HPC systems.
The goal of this architecture is to make porting Balsam to new HPC systems easier: a developer
should **only** have to write minimal interface code under `balsam.platform` (with an appropriate
off-the-shelf config file in `balsam.config.defaults`).

Here is a summary of the key Balsam platform interfaces:

### `AppRun`

This is the application launcher (`mpirun`, `aprun`, `jsrun`) interface used by the Balsam launcher (pilot job). 
It encapsulates the environment, workdir, output streams, compute resource specification (such as MPI ranks and GPUs), and the running process.
`AppRun` implementations may or may not use a subprocess implementation to invoke the run.

### `ComputeNode`

The Balsam launcher uses this interface to discover available compute resources within a batch job, as well as to enumerate resources (CPU cores, GPUs) on a node and track their occupancy.

### `Scheduler`

The Balsam Site uses this interface to interact with the local resource manager (e.g. Slurm, Cobalt) to submit new batch jobs, check on job statuses, and inspect other system-wide metrics (e.g. backfill availability).

### `TransferInterface`

The Balsam Site uses this interface to submit new transfer tasks and poll on their status. A GlobusTransfer interface is implemented for batching Job stage-ins/stage-outs into Globus Transfer tasks.

## `balsam.config`

A comprehensive configuration is stored in each Balsam Site as `settings.yml`.
This *per-site* config improves isolation between sites, and enables more flexible configs when multiple sites share a filesystem.

The Settings are also described by a Pydantic schema, which is used to validate the YAML file every time it is loaded.
The loaded settings are stored in a `SiteConfig` object, defined within this subpackage.

## `balsam.site`

This subpackage contains the real functional core of Balsam: the various components that run on a Site to execute
workflows.

### `JobSource`

Launchers and pre/post-processing modules use this interface to fetch `Jobs` from the API. The abstraction
keeps specific API calls out of the launcher code base, and permits different implementation strategies:

- `FixedDepthJobSource` maintains a queue of pre-fetched jobs using a background process
- `SynchronousJobSource` performs a blocking API call to fetch jobs according to a specification of available resources.

### `StatusUpdater`

The `StatusUpdater` interface is used to manage job status updates, and also helps to keep API-specific code out of the other Balsam internals. The primary implementation `BulkStatusUpdater` pools update events that are passed via queue to a background process, and performs bulk API updates to reduce the frequency of API calls.

### `ScriptTemplate`

The `ScriptTemplate` is used to generate shell scripts for submission to the local resource manager, using a Site-specific job template file.

### `ApplicationDefinition`

Users write their own subclasses of `ApplicationDefinition` to configure the Apps that may run at a particular Balsam Site. These classes are written into Python modules in the Site `apps/` folder. Each `ApplicationDefinition` is automatically synced with the API when users run the `balsam app sync` command.

### `Launcher`

The `MPI` and `serial` job modes of the Balsam launcher are implemented here. These are standalone, executable Python scripts that carry out the execution of Balsam `Jobs` (sometimes called a **pilot job** mechanism). The launchers are invoked from a shell script generated by the `ScriptTemplate` which is submitted to the local resource manager (via the `Scheduler` interface).

## `balsam.site.service`

The Balsam Site daemon comprises a group of background processes that run on behalf of the user. The daemon may run on a login node, or on any other resource appropriate for a long-running background process. The only requirements are that:

- The Site daemon can access the filesystem with the Site directory, and
- The Site daemon can access the local resource manager (e.g. perform `qsub`)
- The Site daemon can access the Balsam API

The Site daemon is organized as a collection of `BalsamService` classes, each of which describes a particular
background process. This setup is highly modular: users can easily configure which service modules are in use, and developers can implement additional services that hook directly into the Site.

### `SchedulerService`

This `BalsamService` component syncs with `BatchJobs` in the Balsam API and uses the `Scheduler` platform interface to submit new `BatchJobs` and update the status of existing `BatchJobs`. **It does not automate the process of job submission** -- it only serves to keep the API state and local resource manager state synchronized.

For example, a user performing the `balsam submit-launch` command causes a new `BatchJob` to be created in the API.
The `SchedulerService` then detects this new `BatchJob`, generates an appropriate script from the `ScriptTemplate`, and submits it to the local Slurm scheduler.

### `ElasticQueueService`

This `BalsamService` monitors the backlog of `Jobs` and locally available compute resources, and it automatically submits new `BatchJobs` to the API to adapt to realtime workloads. This is a form of automated job submission, which works together with the `SchedulerService` to fully automate resource allocation and execution.

### `QueueMaintainerService`

This is another, simpler, form of automated job submission, in which a constant number of fixed-size `BatchJobs` are maintained at a Site (e.g. keep 5 jobs queued at all times). Intended to get through a long campaign of runs.

### `ProcessingService`

This service carries out the execution of various workflow steps that are defined on the `ApplicationDefinition`:

- `preprocess()`
- `postprocess()`
- `handle_error()`
- `handle_timeout()`

These are meant to be lightweight and IO-bound tasks that run in a process pool on the login node or similar resource.
Compute-intensive tasks should be performed in the main body of an App.

### `TransferService`

This service automates staging in data from remote locations prior to the `preprocess()` step of a Job, and staging results out to other remote locations after `postprocess()`. The service batches files and directories that are to be moved between a certain pair of endpoints, and creates batch Transfer tasks via the `TransferInterface`.

## `balsam.cmdline`

The command line interfaces to Balsam are written as Python functions decorated with [Click](https://click.palletsprojects.com/en/7.x/)
