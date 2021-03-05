# Roadmap

## Higher priority 

- [ ] Generalized ElasticQueue implementation
- [X] Server & Client Auth: beyond password-based
- [X] Test runner framework (ability to run tests selectively based on location)
- [ ] Site unit tests & test framework:
  - [ ] Processing
  - [ ] Transfer
  - [ ] Scheduler
  - [ ] Launcher (MPI mode)
  - [ ] Launcher (Serial mode)
  - [ ] Integration test
  - [ ] Throughput benchmark

## Medium priority

- [ ] Polish CLI: old balsam-style `balsam ls` tabular views
- [ ] API "DAG" capability: creating prototype dags and instances of dags
- [X] Type annotations in API
- [ ] DB query profiling & optimization: prefetch loads & bulk writes 


## Web Interface

- [ ] Nuxt / Vuetify Web UI project skeleton 
- [ ] Wire up Basic Auth with Axios+VueX
- [ ] using VueX:websocket plugin for live Job Status updates
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

- [ ] Jupyter API wrappers
- [X] Figure out how to add useful signatures to Model, Manager APIs
  - [X] Explicit `__init__` on models and Jupyter notebook introspection-friendly API


## CI
- [X] black, flake8, mypy
- [X] Server tests
- [X] Client API tests
- [ ] Platform unit tests
- [ ] Site unit tests
- [X] Integration test