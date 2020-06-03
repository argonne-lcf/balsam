# Platform interfaces

## Signal Handler module
Used by launchers and service to determine exit criteria.
A module providing `should_exit()` and `register_handlers()`.

Basic implementation would flip a global `_EXIT_FLAG = True`
on `SIGINT` and `SIGTERM`


## MPIRun

##### Command Template

The MPIRun interface provides a **template** for producing platform-specific
`mpirun` command lines from platform-agnostic `Job` specifications.

##### IO management

Controlling output file creation, redirection of stdout, stderr

##### Start & Kill

- Setting environment variables for the run
- Dispatching the run
- Sending an INT, TERM, or KILL signal
- Polling the status of a run
- Cleanup (close files, release resources, etc)

## NodeResources
- Discover available resources on job startup
- Query busy/idle resources and remaining time
- Granularity: nodes, CPUs, GPUs, occupancy
- `assign()` and `free()` resources
- *Future PMIX Integrations* 
    * `release_nodes()` 
    * `request_nodes()`
    * `extend_walltime()`

##  Scheduler
- qsub()
- qstat()


## Job Template
The Job template takes a *fixed* set of platform-agnostic
parameters and generates a job submission script tailored to the platform.

| Parameter | Description | 
| --------- | ----------- |
| `num_nodes` | Requested number of compute nodes | 
| `wall_time_min` | Requested walltime in minutes | 
| `queue` | Name of queue for submission | 
| `project` | Name of project/allocation for submission | 
| `job_mode` | Launcher job mode | 

Optional *pass-through* parameters can be rendered by the Job Template, with the restrictions that:

  - Every parameter is optional with a sensible default value
  - Parameters are escaped with `shlex.quote` prior to template rendering 

```py3
# Metaclass will check that:
# 1) all required params appear in template
# 2) any optional params are used in the template
class LocalTemplate(JobTemplate):
    template_str = '''...'''
    optional_params = [
        {
            'name': "use_ssd",
            'validators': [str2bool],
            'default': True
        }
    ]
```

