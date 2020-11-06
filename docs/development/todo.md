# Roadmap

## Short Todo

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
- [X] Token Auth
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

- [X] Auth & Client configuration (`login`)
- [X] DB management (`db`)
- [X] Site management (`init mv rm ls rename-host`)
  - [X] Interactive setup: choose a preset config
- [X] App management (`sync`)
- [X] Job List with tag selectors (handling "name" and "workflow" as tags)
- [X] Job create, update, delete, reset, dep
- [ ] BatchJob (`submit-launch`)
- [X] Service
- [X] `which`

## Balsam Site Components

- [X] `JobSource` interface
- [X] `StatusUpdater` interface
- [X] Balsam Service module interface
- [X] Transfer service module
- [X] Processing service module
- [X] Scheduler service module
- [ ] Auto-scaler service module

## Launcher

- [ ] "Thin" platform interfaces: maximize logic in generic launcher code; minimize effort in platform interfaces 
- [ ] Carefully avoid any platform-dependent objects or even patterns in the generic launcher code
  - [ ] No assumptions about 1 mpirun per node in MPI mode; etc...
  - [ ] Do not even assume that an MPIRun has to be a subprocess!

- [ ] Plan Launcher, ComputeNode, and MPIRun interfaces
- [ ] Using only `JobSource` and `StatusUpdater` interfaces
- [ ] `ResourceManager` interface & implementation
  - Tracks `Node` resources
  - Calls `JobSource` `acquire()`
  - Assigns new Jobs to idle resources
  - Keeps track of busy/idle resources as jobs start & finish
- [ ] Dispatch jobs
- [ ] Aggregating updates with `StatusUpdater`; handling timeouts/fails
- [ ] Checking stopping criteria
- [ ] SAFE args to Popen using App Jinja template
- [ ] Simplified exit criterion: nothing to run for N seconds (configurable TTL)


- [ ] Clean launcher cmdline entry point calls into a specific job mode
- [ ] tags-aware. supports "workflow" as a legacy arg (via tag).

- [ ] Support serial and MPI mode
- [ ] Serial mode supports either ZeroMQ or MPI communicator, automatic multi-master (N/128)

- [ ] Both serial and MPI modes support subdividing a node and node-packing, if the ComputeNode allows it
    If the MPIRun is sub-node, set GPU & CPU affinities in the launch command and track allocated/free slots

- [ ] Test launchers on Theta with simple App

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


## CI
Portable CI pipeline (to deploy at Summit, Cori, Theta):

- Server setup
	Download
	Install in venv
	Deploy server at known host:port

- Server tests
	Suite of tests directly against server

- Client API Tests

- Real computing system integration tests: 
	Download
	Install in venv
	Register & login to known host:port
	Create a Site
	Populate Apps and Jobs
		app testing end-to-end flow
	Activate Globus endpoints
	Configure settings to run a queue_maintainer with depth of 1
	Run service

- Monitor logs, job and batchjob states
	report failures in any step
	report ERROR from log
	report failed state in API
	transmit logs, state history, job outputs as artifacts
	Test that transfers actually occurred
