# Project Layout

## Overview
This page summarizes Balsam architecture at a high level in terms of the roles that various modules play.  Here are the files and folders you'll find right under `balsam/`:

- `balsam.schemas`: The source of truth on the REST API schema is here.  This defines the various resources which are imported by FastAPI code in `balsam.server`.  FastAPI uses this to generate the OpenAPI schema and interactive documentation at `/docs`. Moreover, the **user-facing** SDK in `balsam/_api/models.py` is dynamically-generated from the schemas herein. Running `make generate-api` (which is invoked during `make all`) will re-generate `_api/models.py` from the schemas.
- `balsam.server` the backend REST API server code that is deployed using Gunicorn, alongside PostgreSQL, to host the Balsam web service. Virtually all User or Site-initiated actions via `balsam.api` will ultimately create an HTTPS request in `balsam.client` that gets handled by this server.
- `balsam.client`:  Implementation of the lower-level HTTP clients that are **used by** the Managers of `balsam._api`.  Auth and retry logic is here.  This is what ultimately talks to the server.
- `balsam._api`:  implementation of a Django ORM-like library for interacting with the Balsam REST API (e.g. look here to understand how `Job.objects.filter()` is implemented)
- `balsam.api`: public re-export of the user-facing resources in `_api`
- `balsam.analytics`: user-facing helper functions to analyze Job statistics
- `balsam.cmdline`: The command line interfaces.
- `balsam.config.config`: defines the central `ClientSettings` class, which loads credentials from `~/.balsam/client.yml`, as well as the `Settings` class, which loads Site-local settings from `settings.yml`.  To understand the magic in these classes look at [Pydantic Settings management](https://pydantic-docs.helpmanual.io/usage/settings/).
- `balsam.config.site_builder`: Does the heavy lifting for `balsam site init`.
- `balsam.config.defaults`:  The default settings for various HPC platforms that Balsam supports out-of-the-box.  Add new defaults here to extend the list of systems that users are prompted to select from `balsam site init.`
- `balsam.platform`: This package defines generic interfaces to compute nodes, MPI app launchers, and schedulers. Concrete subclasses of these implement the platform-specific interfaces (e.g. Theta `aprun` launcher or Slurm scheduler)
- `balsam.shared_apps`: pre-packaged `ApplicationDefinitions` that users can import, subclass, and deploy to their Sites. 
- `balsam.site`: The implementation of the Balsam Site Agent and the Launcher pilot jobs that it ultimately dispatches. Thanks to the generic platform interfaces, all the code that does heavy lifting here is **totally platform-agnostic**: if you find yourself modifying code in here to accomodate new systems, you're doing it wrong!
- `balsam.util`: logging, signal handling, datetime parsing, and miscellaneous helpers.

## `balsam.schemas`

[Pydantic](https://pydantic-docs.helpmanual.io/) is used to define the REST API data structures, (de-)serialize HTTP JSON payloads, and perform validation. The schemas under `balsam.schemas` are used _both_ by the user-facing `balsam._api` classes and the backend `balsam.server.routers` API. Thus when an update to the schema is made, both the client and server-side code inherit the change.

The script `schemas/api_generator.py` is invoked to re-generate the `balsam/_api/models.py` whenever the schema changes. Thus, users benefit from always-up-to-date docstrings and type annotations across the Balsam SDK, while the implementation is handled by the internal Models, Managers, and Query classes in `balsam/_api`.

## `balsam.server`

This is the self-contained codebase for the API server, implemented with [FastAPI](https://fastapi.tiangolo.com/) and [SQLAlchemy](https://docs.sqlalchemy.org/en/13/).  We do not expect Balsam users to ever run or touch this code, unless they are interested in standing up their own server.

- `server/conf.py` contains the server settings class which loads server settings from the environment using Pydantic.
- `server/main.py` imports all of the API routers which define the HTTP endpoints for `/auth/`, `/jobs`, etc...
- `server/auth` contains authentication routes and logic.
    - `__init__.py::build_auth_router` uses the server settings to determine which login methods will be exposed under the API `/auth` URLs.  
    - `authorization_code_login.py` and `device_code_login.py` comprise the OAuth2 login capability.
    - `db_sessions.py` defines the `get_admin_session` and `get_webuser_session` functions that manage database connections and connection pooling.  The latter can be used to obtain user-level sessions with RLS (row-level security).
    - `token.py` has the JWT logic which is used **on every single request** (not just login!) to authenticate the client request prior to invoking the FastAPI route handler.
- `server.main` defines the top-level URL routes into views located in `balsam.server.routers`
- `balsam.server.routers` defines the possible API actions.  These routes ultimately call into various methods under `balsam.server.models.crud`, where the business logic is defined.
- `balsam.server.models` encapsulates the database and any actions that involve database communication.
    - `alembic` contains the database migrations
    - `crud` contains the business logic invoked by the various FastAPI routes
    - `tables.py` contains the SQLAlchemy model definitions


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

This is the primary user-facing SDK.  Under the hood, it uses a `RESTClient` implementation  from the `balsam.client` subpackage to actually make HTTP requests.

For example, the `Job` model has access via `Job.objects` to an instance of `JobManager` which in turn was initialized with an authenticated `RESTClient`.  The Manager is responsible for auto-chunking large requests, lazy-loading queries, handling pagination transparently, and other features inspired by Django ORM that go beyond a typical auto-generated OpenAPI SDK.  *Rather than emitting SQL, these Models and Managers work together to generate HTTP requests.*

### `ApplicationDefinition`

Users write their own subclasses of `ApplicationDefinition` (defined in
`_api/app.py`) to configure the Apps that may run at a particular Balsam Site.

Each `ApplicationDefinition` is serialized and synchronized with the API when users run the `sync()` method or submit Jobs using the App.

## `balsam.platform`

The `platform` subpackage contains all the **platform-specific** interfaces to various HPC systems.
The goal of this architecture is to make porting Balsam to new HPC systems easier: a developer
should **only** have to write minimal interface code under `balsam.platform` that subclasses and implements well-defined interfaces.

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

The Settings are also described by a Pydantic schema, which is used to validate
the YAML file every time it is loaded.  The loaded settings are held in a
`SiteConfig` instance that's defined within this subpackage. 

The SiteConfig is available to all users via:
`from balsam.api import site_config`.  This import statement depends on the resolution of the Balsam site path from the local filesystem (i.e. it can only work when `cwd` is inside of a Balsam site, which is the case wherever a `Job` is running or pre/post-proccesing.)

## `balsam.site`

This subpackage contains the real functional core of Balsam: the various
components that run on a Site to execute workflows.  The Site `settings.yml`
specifies which platform adapters are needed, and these adapters are *injected*
into the Site components, which use them generically.  This enables all the code
under `balsam.site` to run  across platforms without modification.

### `JobSource`

Launchers and pre/post-processing modules use this interface to fetch `Jobs` from the API. The abstraction
keeps specific API calls out of the launcher code base, and permits different implementation strategies:

- `FixedDepthJobSource` maintains a queue of pre-fetched jobs using a background process
- `SynchronousJobSource` performs a blocking API call to fetch jobs according to a specification of available resources.

Most importantly, both `JobSources` use the Balsam `Session` API to **acquire**
Jobs by performing an HTTP `POST /sessions/{session_id}`.  This prevents race
conditions when concurrent launchers acquire Jobs at the same Site, and it
ensures that Jobs are not locked in perpetuity if a Session expires. Sessions
are ticked with a periodic heartbeat to refresh the lock on long-running jobs.
Eventually, Sessions are deleted when the corresponding launcher ends.  Details of the job acquisition API are in `schemas/sessions.py::SessionAcquire`.

### `StatusUpdater`

The `StatusUpdater` interface is used to manage job status updates, and also helps to keep API-specific code out of the other Balsam internals. The primary implementation `BulkStatusUpdater` pools update events that are passed via queue to a background process, and performs bulk API updates to reduce the frequency of API calls.

### `ScriptTemplate`

The `ScriptTemplate` is used to generate shell scripts for submission to the local resource manager, using a Site-specific job template file.


### `Launcher`

The `MPI` and `serial` job modes of the Balsam launcher are implemented here. These are standalone, executable Python scripts that carry out the execution of Balsam `Jobs` (sometimes called a **pilot job** mechanism). The launchers are invoked from a shell script generated by the `ScriptTemplate` which is submitted to the local resource manager (via the `Scheduler` interface).

The `mpi` mode is a simpler implementation that runs a single process on the
head node of a batch job which launches each job with the MPI launcher (e.g.
`aprun`, `mpiexec`).  The `serial` mode is more involved: a **Worker** process
is first started on each compute node; these workers then receive `Jobs` and
send status updates to a Master process.  The advantage of Serial mode is that
very large numbers of node-local jobs can be launched without incurring the
overhead of a single `mpiexec` per job.

In both `mpi` and `serial` modes, the leader process uses a `JobSource` to acquire jobs and `StatusUpdater` to send state updates back to the API (see above).

Serial mode differs from MPI mode by using ZeroMQ to forward jobs from the
leader to the Workers.  Moreover, users can pass the `--partitions` flag to
split a single Batch job allocation into multiple leader/worker groups.  This
allows for scalable launch to hundreds of thousands of simultaneous tasks.  For
instance on ThetaKNL, one can launch 131,072 simultaneous jobs across 2048
nodes by packing 64 Jobs per node with `node_packing_count=64`.  The Workers
will prefetch Jobs and launch them, thereby hiding the overhead of communication
with the leader.  To further speed this process, the 2048 node batch job can be
split into `16` partitions of `128 nodes` each. Thus: 1 central API server fans
out to 16 serial mode leaders, each of which fan out to 128 workers, and each
worker prefetches Jobs and launches 64 concurrently. Job launch and polling are almost entirely overlapped with communication in this paradigm, and the serial mode leaders use background processes for the `JobSource` and `StatusUpdater`, leaving the leader highly available to forward jobs to Workers and route updates back to the `StatusUpdater`'s queue.

## `balsam.site.service`

The Balsam Site daemon comprises a group of background processes that run on behalf of the user. The daemon may run on a login node, or on any other resource appropriate for a long-running background process. The only requirements are that:

- The Site daemon can access the filesystem with the Site directory, and
- The Site daemon can access the local resource manager (e.g. perform `qsub`)
- The Site daemon can access the Balsam API

The Site daemon is organized as a collection of `BalsamService` classes, each of which describes a particular
background process. This setup is highly modular: users can easily configure which service modules are in use, and developers can implement additional services that hook directly into the Site.

### `main`

The `balsam/site/service/main.py` is the entry point that ultimately loads `settings.yml` into a `SiteConfig` instance, which defines the various services that the Site Agent will run.  Each of these services is launched as a background process and monitored here after invoking `balsam site start`.  When terminated (e.g. with `balsam site stop`) -- the teardown happens here as well.


The following are some of the most common plugins to the Balsam site agent.

### `SchedulerService`

This `BalsamService` component syncs with `BatchJobs` in the Balsam API and **uses the `Scheduler` platform interface** to submit new `BatchJobs` and update the status of existing `BatchJobs`. **It does not automate the process of job submission** -- it only serves to keep the API state and local resource manager state synchronized.

For example, a user performs `balsam queue submit` to add a new `BatchJob` to the REST API.
The `SchedulerService` **eventually** detects this new `BatchJob`, generates an appropriate script from the `ScriptTemplate` and `job-template.sh` (see above), and submits it to the local Slurm scheduler.

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
