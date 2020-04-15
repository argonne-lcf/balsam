Setting up Applications
=======================

The ApplicationDefinition
--------------------------
Each task submitted to Balsam corresponds to the execution of a single `ApplicationDefinition`.  Before adding runs, we have to tell Balsam what 
the application is.  An `ApplicationDefinition` object comprises the following fields:

| Field         |  Description                             | Required?   | 
| ------------- | ---------------------------------------  | ----------  | 
| `name`        | A unique identifier for the application  | **yes**     |
| `executable`  | First half of the command to execute     | **yes**     |
| `description` | Descriptive text for your reference      | *optional*  |
| `preprocess`  | A script that runs prior to execution    | *optional*  |
| `postprocess` | A script that runs after execution       | *optional*  |
| `envscript`   | Script for loading modules, setting envs | *optional*  |

Fully-qualified paths should be used in defining Applications. 
The `executable` can be a simple path to an executable file, or a more
complex, multi-argument command line. The app `executable` is then joined task `args` field is concatenated to the app's `executable` field to form the full command line.   **The key is to understand that
Balsam executes the following shell command for each task:** 

`{application.executable} {task.args}`

`preprocess` and `postprocess` may be used to attach scripts that run before and after an application's main executable.  
The `preprocess` stage runs only once before a task is executed; it does not run again when a task is restarted due to timeout or 
failure. The `postprocess` stage normally runs only after successful execution of a task (application returns error code 0).  A task 
can be configured to error handling by the `postprocess` script by setting `post_error_handler=True` (see below).  These scripts run
in the working directory of each task and have access to the task state via the Balsam Python API:

```python
from balsam.launcher.dag import current_job

def timeout_handler():
    """Run this code before restarting a job that ran out of time"""
    pass

if current_job.state == "RUN_TIMEOUT":
    timeout_handle()
```

Creating ApplicationDefinitions
------------------------------------
You can add Balsam Apps quickly from the command line:
```bash
balsam app --name MyApp --executable '/path/to/app' --preprocess `python /path/to/preproc.py`
```

Or via the Python API:
```python
from balsam.core.models import ApplicationDefinition

myApp = ApplicationDefinition(
    name="myname",
    executable="singularity run /path/to/myImage.sif /bin/app",
    envscript="/path/to/setup-envs.sh",
    postprocess="python /path/to/post.py"
)
myApp.save()
```

Creating Tasks with BalsamJob
-----------------------------

Once your Applications are defined, you can start composing workflows by
adding tasks to the database. Tasks can be added with the
`balsam job` command-line tool:

``` {.bash}
$ balsam job --help # see help menu with listing of fields
$ balsam job --name hello --workflow Test --app sayHi --args "world!" --ranks-per-node 2
```

Or, equivalently, using the `balsam.launcher.dag.BalsamJob()` constructor and Django model save method:

``` {.python}
from balsam.launcher.dag import BalsamJob
job = BalsamJob(
    name = "hello",
    workflow = "hello",
    application = "sayHi",
    args = "world!",
    ranks_per_node = 2,
)
job.save()
```

A powerful concept in Balsam is that you can add tasks from anywhere at any time:

   -   From a login shell, even in the middle of a running job
   -   From inside a pre- or post-processing stage of a task
   -   During the execution of an Application itself (either a system
        call to `balsam job` or direct use of Python API)

Jobs can be modified and removed from the command line (see `balsam rm --help` and `balsam modify --help`) or Python API. 

Balsam uses the Django ORM, and the `BalsamJob` and `ApplicationDefinition` classes are just [ordinary Django models](https://docs.djangoproject.com/en/3.0/topics/db/queries/).  Users are strongly encouraged to read up on writing queries with Django. The API is intuitive and provides flexible methods to query and manipulate the `BalsamJob` table.

See the [FAQs](../faq/recipes.md) for some neat examples and links to further reading.

Balsam State Flow
-----------------

As the Balsam components process your workflow, each task advances
through a series of states according to the flow chart below.

Balsam processes each `BalsamJob` as a state-machine: tasks
proceed from one state to the next according to this flow chart. 
For instance, to re-run a task, set its state to `RESTART_READY`.

![](figs/state-flow.png)

BalsamJob Fields
----------------

| Field         |  Description                                                            |
| ------------- | ----------------------------------------------------------------------  |
| `name`        | Determines working directory. Should form a unique pair with `workflow` |
| `workflow`    | Determines working directory. Should form a unique pair with `name`     |
| `application` | Name of the `ApplicationDefinition` to run with |
| `args` | Command line arguments to the application executable |
| `data` | Arbitrary [JSON data storage].  Useful for storing results together with BalsamJob data |
| `user_workdir` | Override default directory naming scheme with a fully-qualified path |
| `description` | Arbitrary text description to associate with a task |
| `parents`     | IDs of parent jobs (task will not start until dependencies satisfied) |
| `input_files` | glob (wildcard) patterns for files to copy from parent to child job |
| `wall_time_minutes` | Estimated task duration.  Useful to set priority: longer tasks run first. |
| `num_nodes` | Number of nodes on which this task should run (usually 1 unless using MPI) |
| `ranks_per_node` | Number of MPI ranks per node (leave at 1 unless using MPI) |
| `cpu_affinity` | CPU-thread affinity option (on ALCF Theta, use either `depth` or `none`) |
| `threads_per_rank` | Number of threads per MPI rank (on Theta, the aprun `-d` flag) |
| `threads_per_core` | Number of threads per hardware core (on Theta, the aprun `-j` flag) |
| `node_packing_count` | For **non-MPI** tasks and **serial** job mode only:  how many tasks to pack per node |
| `environ_vars` | Colon-separated list (`ENV1=VALUE1:ENV2=VALUE2`) |
| `post_error_handler` | Boolean: whether or not `postprocess` should be invoked to handle `RUN_ERROR` jobs |
| `post_timeout_handler` | Boolean: whether or not `postprocess` should be invoked to handle `RUN_TIMEOUT` jobs |
| `auto_timeout_retry` | Boolean: whether or not `RUN_TIMEOUT` jobs should automatically advance to `RESTART_READY` |
| `state` | Current job state |
| `state_history` | History of job states with timestamps for each transition |

[JSON data storage]: https://docs.djangoproject.com/en/3.0/ref/contrib/postgres/fields/#jsonfield