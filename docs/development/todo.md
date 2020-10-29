# Roadmap

## Short Todo

- Scheduler Interface
  - finish refactoring; use balsam.schema
- Local scheduler class
- Test queue_maintainer and scheduler service modules with local sched; dummy job template


- Transfer service module
- test with a dummy app that bypasses launcher; skip state in preproc

- Double check autoscaler & scheduler service modules: test on theta

- Launcher & NodeDetection interfaces
- Test launchers / local-only workflow on Theta/generic-02

- Test end-to-end (transfer; preproc; run; postproc; stageout) on Theta with backfill autoscaler
- Test on ThetaGPU
- Test on Bebop
- Test on Cooley
- Test on Cori
- Test on Summit

- Push on Auth & secure hosting solution
- API Benchmarking and optimization of key endpoints
- API "DAG" capability: creating prototype dags and instances of dags

- Merge latest balsam0 features (ZMQ/etc...) into master: get a final tagged release of 0.x
- Incorporate balsam0 features into balsam1 
  - push to new "main" branch 
  - make "main" the new default branch on GitHub
  - master will continue to point to the last commit of 0.x

- Improve CLI:  incorporate balsam0 features, refactor, prettify
- Improve API: 
  - Models, ModelFields, Managers should have useful signatures, type hints, not failing in PyLance
	- use ABC abstractmethod in Base Model, Manager

- Nuxt / Vuetify project skeleton using VueX:websocket plugin

## REST API

- [ ] Login Auth
- [ ] Token Auth
- [ ] Idempotent updates
- [ ] `POST` with `submission_id`, response caching, optimizstic lock & rollback mechanism
- [ ] DB query profiling & optimization: prefetch loads & bulk writes 
- [ ] DAG Endpoint: creating prototype DAGs and instances of workflows from the DAG
  0. Focus on critical endpoints only: probably only `Job` bulk create, update, acquire, get
  1. Don't optimize until you have timings on a realistic API call
  2. Use `prefetch` and `select_related`. Consider `defer` to avoid pulling unnecessary rows
  3. Bulk creates and updates
  4. If DB query slow on GET, consider caching in Redis layer
  5. Consider offloading time-consuming processes (Celery+Redis)

## Python API

- [ ] Support typing on Models/Mangers (set Model fields explicitly?)
- [ ] Explicit `__init__` on models and Jupyter notebook introspection-friendly API
- [ ] Any other helpers/wrappers needed in API for Jupyter usage?

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

## Balsam Site Components

- [X ] `JobSource` interface
  - Wraps `Session` API: acquire jobs, tick heartbeat
  - not concerned with dispatch of jobs or assignment of jobs to resources
  - On init, create a new Session (pass `site_id` and `batch_job`)
  - Manage `tick` daemon thread and `release` session on exit
  - Provide `acquire` wrapper method that wraps client.sessions.acquire() directly
- [X ] `StatusUpdater` interface
  - 1 thread + queue for pooling status updates into bulk updates
- [X ] Balsam Service module interface
- [ ] Transfer service module
- [X ] Processing service module
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
