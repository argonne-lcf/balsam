# Roadmap

## REST API

- [x] Data models
- [x] Serializers
- [x] URL routing
- [x] Views, pagination, filtering
- [ ] Login Auth
- [ ] Token Auth
- [x] `APIClient` tests
- [ ] Idempotent updates
- [ ] `POST` with `submission_id`, response caching, optimizstic lock & rollback mechanism
- [ ] DB query profiling & optimization: prefetch loads & bulk writes 0. Focus on critical endpoints only: probably only `Job` bulk create, update, acquire, get
  1. Don't optimize until you have timings on a realistic API call
  2. Use `prefetch` and `select_related`. Consider `defer` to avoid pulling unnecessary rows
  3. Bulk creates and updates
  4. Can bypass serializers entirely with `queryset.values()`
  5. Cache model instances to avoid DB on GET requests
- [ ] Cache user info, sites, apps, batchjobs, sessions by owner. used for `GET` requests and request validation
- [ ] Offload longer processing (e.g. post Job update via Celery+Redis)
- [ ] cron+PIDfile deployment of PG+Gunicorn on generic-01
- [ ] Replace JSONFilters with Django-Filter plugins limited to str:str mappings
- [ ] DAG Endpoint: creating prototype DAGs and instances of workflows from the DAG

## LiveServer Test Harness

- [ ] Completely disable PyTest-Django. Run separate Gunicorn server and separate setup/teardown
- [ ] Timeout+retry tests (to trigger POST rollback)
- [ ] Concurrency tests with multiprocessing & `RequestsClient`

## Client

- [x] RESTClient base class
- [x] DirectAPI Client
- [x] Requests Client
- [x] Resource components and URL-mapped methods
- [ ] Update Resources to reflect API changes

## Python API

- [ ] Update models & managers to reflect API changes
- [ ] Update API Test cases

## CLI

- [ ] Auth & Client configuration (`login`)
- [ ] DB management (`db`)
- [ ] Site management (`init mv rm ls rename-host`)
  - [ ] Interactive setup: choose a preset config
- [ ] App management (`sync`)
- [ ] Job List with tag selectors (handling "name" and "workflow" as tags)
- [ ] Job create, update, delete, reset, dep
- [ ] BatchJob (`submit-launch`)
- [ ] Service
- [ ] `which`

## Configuration

- [ ] Configuration structure
  - APIClient should be independently configurable for Launcher & Service
    - Launcher may need to use HTTP Proxy
- [ ] SiteConfig methods
- [ ] Default presets
  - [ ] Local
  - [ ] Theta
  - [ ] Cooley
  - [ ] Bebop
  - [ ] Cori
  - [ ] Summit
  - [ ] Theta-GPU
  - [ ] Crux
  - [ ] Perlmutter
  - [ ] Aurora
  - [ ] Frontier

## Balsam Site Components

- [ ] `JobSource` interface
  - Wraps `Session` API: acquire jobs, tick heartbeat
  - not concerned with dispatch of jobs or assignment of jobs to resources
  - On init, create a new Session (pass `site_id` and `batch_job`)
  - Manage `tick` daemon thread and `release` session on exit
  - Provide `acquire` wrapper method that wraps client.sessions.acquire() directly
- [ ] `StatusUpdater` interface
  - 1 thread + queue for pooling status updates into bulk updates
- [ ] Balsam Service module interface
- [ ] Transfer service module
- [ ] Processing service module
- [ ] Scheduler service module
- [ ] Auto-scaler service module

## Launcher

- [ ] Using only `JobSource` and `StatusUpdater` interfaces
- [ ] `ResourceManager` interface & implementation
  - Tracks `Node` resources
  - Calls `JobSource` `acquire()`
  - Assigns new Jobs to idle resources
  - Keeps track of busy/idle resources as jobs start & finish
- [ ] Dispatch jobs
- [ ] Aggregating updates with `StatusUpdater`; handling timeouts/fails
- [ ] Checking stopping criteria
- [ ] Safe Popens using App Jinja template
- [ ] tags-aware. supports "workflow" as a legacy arg (via tag).

## Platform Interfaces

- [ ] MPIRun
  - [ ] Local
  - [ ] Theta
  - [ ] Cooley
  - [ ] Bebop
  - [ ] Cori
  - [ ] Summit
  - [ ] Theta-GPU
  - [ ] Crux
  - [ ] Perlmutter
  - [ ] Aurora
  - [ ] Frontier
- [ ] JobTemplate
  - [ ] Local
  - [ ] Theta
  - [ ] Cooley
  - [ ] Bebop
  - [ ] Cori
  - [ ] Summit
  - [ ] Theta-GPU
  - [ ] Crux
  - [ ] Perlmutter
  - [ ] Aurora
  - [ ] Frontier
- [ ] Node
  - [ ] Local
  - [ ] Theta
  - [ ] Cooley
  - [ ] Bebop
  - [ ] Cori
  - [ ] Summit
  - [ ] Theta-GPU
  - [ ] Crux
  - [ ] Perlmutter
  - [ ] Aurora
  - [ ] Frontier
- [ ] Scheduler
  - [ ] Local
  - [ ] Theta
  - [ ] Cooley
  - [ ] Bebop
  - [ ] Cori
  - [ ] Summit
  - [ ] Theta-GPU
  - [ ] Crux
  - [ ] Perlmutter
  - [ ] Aurora
  - [ ] Frontier

## Web Interface

- [ ] Nuxt project skeleton
- [ ] Wire up Basic Auth with Axios+VueX
- [ ] Set up VueX store and actions
- [ ] Sites view (grid layout of Site cards)
- [ ] Apps view
- [ ] BatchJobs view (list)
- [ ] BatchJobs view (detail: show related Jobs & utilization plot for the Job)
- [ ] Jobs datatable
- [ ] Interactive Job creation form
- [ ] Transfers view
- [ ] History view: graphical
  - filter controls: by Site, by Tags, by Date Range
  - show: throughput, utilization, available nodes

## Jupyter Interface

- Widgets and plotting convenience functions
