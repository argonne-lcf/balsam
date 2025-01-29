# Defining Applications

## Registering `ApplicationDefinitions`
Once you have a Site, the next step is to define the applications that Balsam may run.
Each Site is linked to a set of `ApplicationDefinition` Python classes: check the [Getting Started tutorial](../../tutorials/theta#set-up-your-apps) to see a quick example of this in action.
You can create and register `ApplicationDefinition` subclasses from any Python
session.  For instance, Balsam supports interactive workflows in Jupyter
notebooks, or workflows driven by any other Python software.

At a minimum, `ApplicationDefinitions` must declare the `site` and a
`command_template` for a shell command:

```python
from balsam.api import ApplicationDefinition

class Sleeper(ApplicationDefinition):
    site = "polaris"
    command_template = 'sleep {{ sleeptime }} && echo goodbye'

Sleeper.sync()
```

The `site` attribute must be present on each `ApplicationDefinition` subclass to identify where the app runs.  The `site` can take any of the following types unambiguously specifying a Site:

- Site name (uniquely assigned during `balsam site init`, like `"polaris"`)
- Site ID (e.g. `142`)
- `Site` object (fetched from the [API](./api.md))

The `ApplicationDefinition` is uniquely identified by its **class name and site**.   In the above example, we defined the `Sleeper` application at the `polaris` Site.  When `Sleeper.sync()` is called, the Python class and associated metadata is *serialized* and shipped to the Balsam web API. The `Sleeper` `ApplicationDefinition` is thereafter linked to the Site named `polaris`, and workflows can proceed to submit `Sleeper` Jobs to `polaris` from anywhere!

!!! warning "`ApplicationDefinitions` must be named uniquely!"
    If another Python session syncs a different `Sleeper` class belonging to
    the same Site, **the previous application will be overwritten!**  This is
    because apps are uniquely resolved by the pair (`site`, `class_name`).  This
    is typically the desired behavior: simply running `sync()` 
    will ensure that Balsam applications stay
    up-to-date with your source code.  However, inadvertent name collisions can
    cause unexpected results when you overwrite the implementation of an
    existing `ApplicationDefinition`.

## The `submit()` shortcut
To run an application, we then [submit
a
**Job**](./jobs.md) to invoke the command with specific resources and
parameters. The `submit()` class method provides a convenient syntax to combine the `sync` initialization step with job submission in a single call:

```python
# Implicitly sync (updates or creates the App) and submit a Job:
job = Sleeper.submit(workdir="test/sleep_3", sleeptime=3)
```

This shorthand syntax is completely equivalent to the following:
```python
from balsam.api import Job

Sleeper.sync()

job = Job(workdir="test/sleep_3", app_id=Sleeper, parameters={"sleeptime": 3})
job.save()
```

It's more efficient to [use bulk-creation](./api.md#creating) to submit large numbers of Jobs in a single network call.  This is possible by passing the special keyword argument `save=False`:

```python
jobs = [
    Sleeper.submit(workdir=f"test/{i}", sleeptime=3, save=False)
    for i in range(100)
]
jobs = Job.objects.bulk_create(jobs)
```

Besides the special `save` kwarg, The `submit()` method has the same signature as the [`Job()` constructor which is covered in-depth on the next page](./jobs.md).

## Python Applications

Besides running shell commands, Balsam can run Python applications directly on the compute nodes.  This paradigm significantly cuts down
boilerplate and reduces the need for creating "entry point" scripts.

Instead of using `command_template`, the `ApplicationDefinition` can simply
define a `run()` method that will be launched using the exact same rules as ordinary shell applications.

```python
class Adder(ApplicationDefinition):
    site = "polaris"

    def run(self, x, y):
        return x + y

job = Adder.submit("test/5plus5", x=5, y=5)
assert job.result() == 10  # This will block until a BatchJob starts
```

`run` is an instance method and should take `self` as the first argument.  Additional positional or keyword arguments can be supplied as well.  When submitting `Jobs`, the parameters are serialized (under the hood `job.parameters = {"x": 5, "y": 5}` is converted to a Base64-encoded byte string with `dill` and stored as part of the `Job` in the Balsam web API.)

The submitted `Jobs` behave *partially* like `concurrent.futures.Future` objects: namely, the return value or Exception raised by `run()` will propagate to the `result()` method.  Refer to the [Jobs documentation](./jobs.md) for more details on these `Future`-like APIs.

### Python App Capabilities and Limitations

Python `run()` function-based `ApplicationDefinitions` enjoy all the same
lifecycle hooks and flexible resource launching capabilities as ordinary
`ApplicationDefinitions`.  For instance, your Balsam apps can directly call into
`mpi4py` code and be launched onto multiple compute nodes:

```python
import numpy as np


class NumpyPlusMPI(ApplicationDefinition):
    site = "polaris"

    def run(self, *dims):
        from mpi4py import MPI
        if MPI.COMM_WORLD.Get_rank() == 0:
            return np.random.rand(*dims)


job = NumpyPlusMPI.submit(
    workdir="test/parallel", 
    dims=[5],
    ranks_per_node=4,
)
```

The `run` function can generally refer to imported modules and objects from other namespaces, **as long as they are importable in the Balsam Site's Python environment**.  This is a crucial constraint to understand when writing `ApplicationDefinitions`: all import statements are essentially *replayed* in the Balsam Site environment when the `ApplicationDefinition` is loaded and de-serialized.  This will fail if any any object referenced by the `ApplicationDefinition` cannot be imported from `sys.path`!

!!! warning "Import statements will replay on the Balsam Site"
    The issues above are easily avoided by ensuring that your codes and their dependencies are installed in the Python virtual environment where the Balsam Site runs.  
    
    Just remember that the Balsam app serializer does not recurse and ship other modules over the wire. **Virtually any import statement referenced by the `ApplicationDefintion` must work on both sides.**

The same constraint holds true when the `ApplicationDefinitions` themselves are located in an imported module.  For example, if your Balsam code is packaged in `my_science_package`, that module/package must be installed in the Site environment where the `ApplicationDefinition` is synced to.

```python
# This import line must work on polaris...
from my_science_package import setup_workflow

# If this method syncs apps to polaris:
setup_workflow(site="polaris")
```

!!! note "Technical Details"
    Under the hood, all `ApplicationDefinitions`, `Job` parameters, return values, and exceptions are serialized and deserialized using `dill` with the `recurse=True` option.

Besides the issue of software dependencies, the following additional limitations should be kept in mind when writing `ApplicationDefinitions`:

- **Do not use any `multiprocessing.Process`.** You can spawn background `Thread` tasks, but `Process` workers will attempt to communicate using `pickle` in a fashion that is incompatible with the serialization scheme used by Balsam.
- **Do not use the `super()` syntax anywhere in the `ApplicationDefinition`.**  This is a [known issue with `dill`](https://github.com/uqfoundation/dill/issues/300).  If you are writing your own class hierarchies, you can always get around this by referencing the parent class directly.
- **Avoid referring to `balsam.api` in the `ApplicationDefinition`.** Instead, you will need to manually initialize the client to fetch resources as follows:
    ```python
    from balsam.config import SiteConfig
    client = SiteConfig().client  # Loads a fresh RESTClient
    Job = client.Job  # Use just like balsam.api.Job
    ```
- **`ApplicationDefinitions`, parameters, return values, and exceptions should be lean.**  Do not rely on the Balsam API to move large quantities of data (instead, the [Transfer API](#transfer-slots) is designed for easy interoperability with out-of-band transfer methods like Globus).  Balsam imposes limits on the order of 100KB for each serialized entity.

## Listing and Refreshing Applications
You can check the Apps registered at a site using the CLI:

```bash
# List apps across all Sites
$ balsam app ls --site=all
```

!!! warning "Restart Sites to Reload `ApplicationDefinitions`"
    A new `ApplicationDefinition` will be automatically loaded by a running
    Site agent or launcher BatchJob.  However, if you modify an *existing* app
    while it is loaded in a running Site or launcher, the change will not 
    propagate!  You **must** remember to run `balsam site sync` to refresh
    the loaded apps in a running Site.

## Loading Existing `ApplicationDefinitions`

You don't need a handle on the `ApplicationDefinition` source code to submit
`Jobs` with it.  Instead, the `app_id` argument to `Job()` can take an application by 
name (string), ID (integer), or reference to a loaded `ApplicationDefinition`.
For example, you can load the set of applications registered at a given Site as follows:

```python
apps_by_name = ApplicationDefinition.load_by_site("polaris")
Sleeper = apps_by_name["Sleeper"]
```

`ApplicationDefinitions` can then be used from this dictionary as if you had
defined them in the current Python session.

## Writing ApplicationDefinitions

At their simplest, `ApplicationDefinitions` provide a declarative template for a
shell command and its adjustable parameters. Alternatively, they define a Python `run()` function that takes arbitrary inputs. To run an application, we submit a
Job that provides values for these parameters. 

Importantly, we **do not** specify how the application is launched (`mpiexec`)
or its CPU/GPU resources in the `ApplicationDefinition`.  Instead, Balsam takes
care of managing resources and building the command lines to efficiently launch our Jobs.

Besides the fundamental `site`, `command_template`, and `run` attributes discussed above, `ApplicationDefinitions`
provide other special attributes and methods that we can override to build more
complex and useful workflow components.

### The Class Path

Balsam Apps are uniquely identified by:

1. The `Site` that they belong to
2. Their `ApplicationDefinition` class `__name__`

For instance, the `Sleeper` application we defined above in `test.py` has a `name` of `Sleeper`. We use this to uniquely identify each `ApplicationDefinition` class later on.

### The Description

The docstring that follows the `class` statement is captured by Balsam 
and stored as a `description` with the REST API.  This is purely
human-readable text that can be displayed in your App catalog.

```python hl_lines="2-4"
class MySimulation(ApplicationDefinition):
    """
    Some description of the app goes here
    """
```

### The Site Identifier

The `site` attribute is required on **all** `ApplicationDefinitions` and it must unambiguously refer to one of your existing Sites.  This class attribute can be a string (site name), integer (site ID), or a `Site` object loaded from the Balsam SDK.

```python hl_lines="5"
class MySimulation(ApplicationDefinition):
    """
    Some description of the app goes here
    """
    site = "polaris"
```

### Environment Variables

The `environment_variables` attribute should be a `Dict[str, str]`
mapping environment variable names to values.  This is useful for
constant environment variables that **do not vary** across runs.
This environment is merged with the environment established in the job 
template.

```python hl_lines="6-8"
class MySimulation(ApplicationDefinition):
    """
    Some description of the app goes here
    """
    site = "polaris"
    environment_variables = {
        "HDF5_USE_FILE_LOCKING": "FALSE",
    }
```

### Command Template
As we have seen, `ApplicationDefinitions` must contain *either* a `command_template` or a `run()` method.  These are mutually exclusive: you must set one or the other. The `command_template` is interpreted as a Jinja2 template; therefore,
parameters must be enclosed in double-curly braces.

```python hl_lines="9"
class MySimulation(ApplicationDefinition):
    """
    Some description of the app goes here
    """
    site = "polaris"
    environment_variables = {
        "HDF5_USE_FILE_LOCKING": "FALSE",
    }
    command_template = "/path/to/simulation.exe -inp {{ input_filename }}"
```

By default, all app parameters are **required** parameters: it is an error
to omit any parameter named in the template.  We can change this behavior below.

### The `run` function

When the `ApplicationDefinition` contains a `run()` method, this function is launched onto compute resources using the parameters set on the corresponding `Job`.

```python hl_lines="6-7"
import numpy as np

class VecNorm(ApplicationDefinition):
    site = "polaris"

    def run(self, vec):
        return np.linalg.norm(vec)
```

### Python executable

When using a `run()` function, it is important that the execution-side Python
environment has the necessary dependencies installed. The optional class
attribute `python_exe` defaults to `sys.executable` and should not be changed if
the app runs in the same environment Balsam is installed in. 

You should override `python_exe` if you wish to invoke the `run` function using a *different* Python environment from the one in which Balsam is installed.  This setting has no effect for `command_template` apps.

```python hl_lines="5"
import numpy as np

class VecNorm(ApplicationDefinition):
    site = "polaris"
    python_exe = "/path/to/bin/python3.8"

    def run(self, vec):
        import numpy as np # Loaded from `python_exe`
        return np.linalg.norm(vec)
```

### Parameter Spec

Maybe we want to have some **optional** parameters in the `command_template`,
which take on a default value in the absence of a value specified in the Job.
We can do this by providing the `parameters` dictionary:

```python hl_lines="10-17"
class MySimulation(ApplicationDefinition):
    """
    Some description of the app goes here
    """
    site = "polaris"
    environment_variables = {
        "HDF5_USE_FILE_LOCKING": "FALSE",
    }
    command_template = "/path/to/sim.exe --mode {{ mode }} -inp {{ input_filename }}"
    parameters = {
        "input_filename": {"required": True},
        "mode": {
            "required": False, 
            "default": "explore", 
            "help": "The simulation mode (default: explore)",
        }
    }
```

Notice that parameters are either required, in which case it doesn't make
sense to have a default value, or not. If a parameter's `required` value is `False`, you **must** provide a `default` value that is used when the parameter is not passed.

The `help` field is another optional, human-readable field, to assist with
App curation in the Web interface.

!!! note "Valid Python Identifiers"
    App parameters can only contain valid Python identifiers, so names with `-`, for instance, will be rejected when you 
    attempt to run `balsam app sync`.

### Transfer Slots

A core feature of Balsam, described in more detail in the [Data Transfers section](./transfer.md), is the ability to write distributed workflows, where
data products move between Sites, and Jobs can be triggered when data arrives at its destination.

We create this behavior starting at the `ApplicationDefinition` level, by defining **Transfer Slots** for data that needs to be **staged in** before or **staged out** after execution.  You can think of the Job workdir as an ephemeral sandbox where data arrives, computation happens, and then results are staged out to a more accessible location for further analysis.

Each `ApplicationDefinition` may declare a `transfers` dictionary, where each
string key names a Transfer Slot.

```python hl_lines="2-17"
class MySimulation(ApplicationDefinition):
    transfers = {
        "input_file": {
            "required": True,
            "direction": "in",
            "local_path": "input.nw",
            "description": "Input Deck",
            "recursive": False,
        },
        "result": {
            "required": True,
            "direction": "out",
            "local_path": "job.out",
            "description": "Calculation stdout",
            "recursive": False
        },
    },
```

In order to fill the slots, each `Job` invoking this application must then provide concrete URIs of the external files:

```python hl_lines="4-8"
Job.objects.create(
    workdir="ensemble/1",
    app_name="sim.MySimulation",
    transfers={
        # Using 'laptop' alias defined in settings.yml
        "input_file": "laptop:/path/to/input.dat",
        "result": "laptop:/path/to/output.json",
    },
)
```

Transfer slots with `required=False` are optional when creating Jobs.
The `direction` key must contain the value `"in"` or `"out"` for stage-in and stage-out, respectively.
The `description` is an optional, human-readable parameter to assist in App curation. The `recursive` flag should be `True` for directory transfers; otherwise, the transfer is treated as a single file. 

Finally, `local_path` must always be given **relative to the Job workdir**.  When `direction=in`, the `local_path` refers to the transfer *destination*.  When `direction=out`, the `local_path` refers to the transfer *source*. 
This `local_path` behavior encourages a pattern where files in the working directory are always named identically, and only the *remote* sources and destinations vary. If you need to stage-in remote files *without renaming* them, a `local_path` value of `.` can be used.

After running `balsam app sync`, the command `balsam app ls --verbose` will show any transfer slots registered for each of your apps.

### Cleanup Files

In long-running data-intensive workflows, a Balsam site may exhaust its HPC storage
allocation and trigger disk quota errors.  To avoid this problem, valuable data
products should be packaged and staged out, while   intermediate files are
periodically deleted to free storage space.  The Site `file_cleaner` service can
be enabled in `settings.yml` to safely remove files from working directories of
finished jobs.  Cleanup does not occur until a job reaches the `JOB_FINISHED`
state, after all stage out tasks have completed.

By default, the `file_cleaner` will not delete anything, even when it has been enabled.  The `ApplicationDefinition` must *also* define a list of glob patterns in the `cleanup_files` attribute, for which matching files will be removed upon job completion.

```python hl_lines="10"
class MySimulation(ApplicationDefinition):
    """
    Some description of the app goes here
    """
    site = "polaris"
    environment_variables = {
        "HDF5_USE_FILE_LOCKING": "FALSE",
    }
    command_template = "/path/to/simulation.exe -inp {{ input_filename }}"
    cleanup_files = ["*.hdf", "*.imm", "*.h5"]
```

Cleanup occurs once for each finished Job and reads the list of deletion patterns from the `cleanup_files` attribute in the `ApplicationDefinition`.

## Job Lifecycle Hooks

The `ApplicationDefinition` class provides several *hooks* into stages of the
[Balsam Job lifecycle](./jobs.md), in the form of overridable methods on the
class.  These methods are called by the Balsam Site as it handles your Jobs,
advancing them from `CREATED` to `JOB_FINISHED` through a series of state
transitions.

To be more specific, an *instance* of the `ApplicationDefinition` class is
created for each `Job` as it undergoes processing. The hooks are called as ordinary
*instance methods*, where `self` refers to an `ApplicationDefinition` object handling a particular `Job`.  The current `Job` can be accessed via the `self.job` attribute (see examples below).  Of course, you
may define any additional methods on the class and access them as usual.


!!! note "`ApplicationDefinitions` are not persistent!"
    `ApplicationDefinition` instances are created and torn down after each invocation of a hook for a particular Job.  This is because they might execute days or weeks apart on different physical hosts.  Therefore, any data that you set on the `self` object within the hook will *not* persist.
    Instead, hooks can persist arbitrary JSON-serializable data on the `Job` object itself via `self.job.data`.

Hook methods are always executed in the current `Job`'s working directory with
stdout/stderr routed into the file `balsam.log`.  All of the methods described
below are **optional**: the default implementation is essentially a
no-op that moves the `Job` state forward.  However, if you *do* choose to
override a lifecycle hook, it is your responsibility to set the `Job` state
appropriately (e.g. you must write `self.job.state = "PREPROCESSED"` in the `preprocess()`
function).  The reason for this is that hooks may choose to *retry* or *fail* a
particular state transition; the `ApplicationDefinition` should be the explicit
source of truth on these possible actions.


### The Preprocess Hook

The `preprocess` method advances jobs from `STAGED_IN` to `PREPROCESSED`.  This represents an opportunity to run lightweight or I/O-bound code on the login node after any data for a Job has been staged in, and *before* the application begins executing. This runs in the `processing` service on the host where the Site Agent is running.

In the following example, `preprocess` is used to read some user-defined data from the `Job` object, attempt to generate an input file, and advance the job state only if the generated input was valid.

```python
class MySimulation(ApplicationDefinition):

    def preprocess(self):
        # Can read anything from self.job.data
        coordinates = self.job.data["input_coords"]

        # Run arbitrary methods defined on the class:
        success = self.generate_input(coordinates)

        # Advance the job state
        if success:
            # Ready to run
            self.job.state = "PREPROCESSED"
        else:
            # Fail the job and attach searchable data
            # to the failure event
            self.job.state = "FAILED"
            self.job.state_data = {"error": "Preproc got bad coordinates"}
```


### The Shell Preamble
The `shell_preamble` method can return a **multi-line string** *or* a **list of
strings**, which are executed in an ephemeral `bash` shell immediately preceding the
application launch command.  This hook directly affects the environment of the
`mpirun` (or equivalent) command used to launch each Job; therefore, it is
appropriate for loading modules or exporting environment variables in an App- or
Job-specific manner. Unlike `preprocess`, this hook is executed by the launcher (pilot
job) on the application launch node.  

```python
class MySimulation(ApplicationDefinition):

    def shell_preamble(self):
        return f'''
        module load conda/tensorflow
        export FOO={self.job.data["env_vars"]["foo"]}
        '''
```

### The Postprocess Hook

The `postprocess` hook is exactly like the `preprocess` hook, except that it
runs **after** Jobs have successfully executed.  In Balsam a "successful
execution" simply means the application command return code was `0`, and the
job is advanced by the launcher from `RUNNING` to `RUN_DONE`. Some common patterns in the `postprocess` hook include: 

- parsing output files
- summarizing/archiving useful data to be staged out
- persisting data on the `job.data` attribute
- dynamically creating *additional* `Jobs` to continue the workflow


Upon successful postprocessing, the job state should be advanced to
`POSTPROCESSED`.  However, a return code of 0 does not necessarily imply a
successful run. The method may therefore choose to set a job as
`FAILED` (to halt further processing) or `RESTART_READY` (to run again, perhaps
after changing some input).

```python
class MySimulation(ApplicationDefinition):

    def postprocess(self):
        with open("out.hdf") as fp:
            # Call your own result parser:
            results = self.parse_results(fp)

        if self.is_converged(results):
            self.job.state = "POSTPROCESSED"
        else:
            # Call your own input file fixer:
            self.fix_input()
            self.job.state = "RESTART_READY"
```

### Timeout Handler
We have just seen how the `postprocess` hook handles the return code `0` scenario by moving jobs from `RUN_DONE` to `POSTPROCESSED`.  There are two less happy scenarios that Balsam handles:

1. The launcher wallclock time expired and the Job was terminated while still running.  The launcher marks the job state as `RUN_TIMEOUT`.
2. The application finished with a nonzero exit code. This is interpreted by the launcher as an *error*, and the job state is set to `RUN_ERROR`.

The `handle_timeout` hook gives us an opportunity to manage timed-out jobs in
the `RUN_TIMEOUT` state. The *default* Balsam action is to immediately mark the
timed out job as `RESTART_READY`: it is simply eligible to run again as soon as
resources are available.  If you wish to *fail* the job or tweak inputs before running again, this is the right place to do it.  

In this example, we choose to mark the timed out job as `FAILED` but dynamically generate a follow-up job with related parameters.

```python
from balsam.api import Job

class MySimulation(ApplicationDefinition):

    def handle_timeout(self):
        # Sorry, not retrying slow runs:
        self.job.state = "FAILED"
        self.job.state_data = {"reason": "Job Timed out"}

        # Create another, faster run:
        new_job_params = self.next_run_kwargs()
        Job.objects.create(**new_job_params)
```

### Error Handler
The `handle_error` hook handles the second scenario listed in the previous
section: when the job terminates with a nonzero exit code.  If you can fix the
error and try again, set the job state to `RESTART_READY`; otherwise, the
default implementation simply fails jobs that encountered a `RUN_ERROR` state.

The following example calls some user-defined `fix_inputs()` to retry a failed
run up to three times before declaring the job as `FAILED`.

```python
class MySimulation(ApplicationDefinition):

    def handle_error(self):
        dat = self.job.data
        retry_count = dat.get("retry_count", 0)

        if retry_count <= 3:
            self.fix_inputs()
            self.job.state = "RESTART_READY"
            self.job.data = {**dat, "retry_count": retry_count+1}
        else:
            self.job.state = "FAILED"
            self.job.state_data = {"reason": "Exceeded maximum retries"}
```

!!! warning "Be careful when updating `job.data`!"
    Notice in the example above that we did not simply update `self.job.data["retry_count"]`, even though that's the only value that changed.  Instead, we created a **new dictionary** *merging* the existing contents of `data` with the incremented value for `retry_count`. If we had attempted the former method, **`job.data` would not have been updated**.

    This is a subtle consequence of the Balsam Python API, which [*tracks*
    mutated data](https://docs.python.org/3/howto/descriptor.html) on the `Job` object whenever a new value is assigned to one of the object's fields. This works great for immutable values, but unfortunately, updates to mutable fields (like appending to a list or setting a new key:value pair on a dictionary)  are not currently intercepted.
    
    The Balsam `processing` service that runs these lifecycle hooks inspects
    mutations on each `Job` and propagates efficient bulk-updates to the REST
    API.