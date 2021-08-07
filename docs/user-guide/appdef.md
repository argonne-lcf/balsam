# Defining Applications

## Adding App Files
Once you have a Site installed, the next logical step is to define the applications that Balsam may run.
Each Site's applications are defined by the set of `ApplicationDefinition` classes in the `apps/` directory.
At a minimum, `ApplicationDefinitions` declare the template for a
shell command and its adjustable parameters.  To run an application, we then submit a
**Job** that provides values for these parameters.

You may add `ApplicationDefinition` subclasses to Python module files (`*.py`)
in the `apps/` folder, with multiple apps per file and/or multiple files.  Every
Site comes "pre-packaged" with some demonstrative Apps. The intention is for you
to copy one of the existing `ApplicationDefinitions` as a starting point, and
adapt it to your own needs.

Balsam also provides a CLI to generate a *starter* `ApplicationDefinition` based on a 
simple application command line invocation:

```bash
$ balsam app create
Application Name (of the form MODULE.CLASS): test.Sleeper
Application Template: sleep {{ sleeptime }} && echo goodbye
```

Now open `apps/test.py` and see the `Sleeper` class that was generated for you.
The allowed parameters for this App are given in double curly braces: `{{
sleeptime }}`.  When you add `test.Sleeper` jobs, you will have to pass this
parameter and Balsam will take care of building the command line.

```python
# apps/test.py
from balsam.site import ApplicationDefinition

class Sleeper(ApplicationDefinition):
    """
    Application description
    """
    command_template = 'sleep {{ sleeptime }} && echo goodbye'
```

Whenever you change the set of `ApplicationDefinitions` in a Site, you must push these changes to 
the backend and reload the Site, using the `app sync` CLI:

```bash
# Refresh Apps:
$ balsam app sync

# And now list them:
$ balsam app ls
```

!!! warning
    Similar to the BatchJob template, the set of ApplicationDefinitions is
    loaded in memory upon Site startup.  You **must** remember to run `balsam
    app sync` to apply changes to the running Site.  Otherwise, new or modified 
    Apps will not run correctly.

    Note that the API does not store anything about the
    `ApplicationDefinition` classes other than the class name and some metadata
    about allowed parameters, allowed data transfers, etc...  What actually runs
    at the Site is loaded solely from the class on the local filesystem.

## Writing ApplicationDefinitions

At their simplest, `ApplicationDefinitions` provide a declarative template for a
shell command and its adjustable parameters.  To run an application, we submit a
Job that provides values for these parameters. 

Importantly, we **do not** specify how the application is launched (`mpiexec`)
or its CPU/GPU resources in the `ApplicationDefinition`.  Instead, Balsam takes
care of managing resources and building the command lines to efficiently launch our Jobs.

Besides the fundamental `command_template` shown above, `ApplicationDefinitions`
provide other special attributes and methods that we can override to build more
complex and useful workflow components.

### The Class Path

Balsam Apps are uniquely identified by:

1. The `Site` that they belong to
2. Their `class_path`, written as `module_name.CLASS_NAME` 

For instance, the `Sleeper` application we defined above in `test.py` has a `class_path` of `test.Sleeper`. We use this to uniquely identify each `ApplicationDefinition` class later on.

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

### Environment Variables

The `environment_variables` attribute should be a `Dict[str, str]`
mapping environment variable names to values.  This is useful for
constant environment variables that **do not vary** across runs.
This environment is merged with the environment established in the job 
template.

```python hl_lines="5-7"
class MySimulation(ApplicationDefinition):
    """
    Some description of the app goes here
    """
    environment_variables = {
        "HDF5_USE_FILE_LOCKING": "FALSE",
    }
```

### Command Template
As we have seen, the central (and only required) class attribute is the `command_template`.  This is interpreted as a Jinja2 template; therefore,
parameters must be enclosed in double-curly braces.

```python hl_lines="8"
class MySimulation(ApplicationDefinition):
    """
    Some description of the app goes here
    """
    environment_variables = {
        "HDF5_USE_FILE_LOCKING": "FALSE",
    }
    command_template = "/path/to/simulation.exe -inp {{ input_filename }}"
```

By default, all app parameters are **required** parameters: it is an error
to omit any parameter named in the template.  We can change this behavior below.

### Parameter Spec

Maybe we want to have some **optional** parameters in the `command_template`,
which take on a default value in the absence of a value specified in the Job.
We can do this by providing the `parameters` dictionary:

```python hl_lines="9-16"
class MySimulation(ApplicationDefinition):
    """
    Some description of the app goes here
    """
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

```python
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
        }
    },
```

### Cleanup Files

## Job Lifecycle Hooks

### Preprocess
- `preprocess()` will run on Jobs immediately before `RUNNING`

### The Shell Preamble
- `shell_preamble()` takes the place of the `envscript`: return a multiline string envscript or a `list` of commands

### Postprocess
- `postprocess()` will run on Jobs immediately after `RUN_DONE`

### Timeout Handler
- `handle_timeout()` will run immediately after `RUN_TIMEOUT`

### Error Handler
- `handle_error()` will run immediately after `RUN_ERROR`

## General class features

- Define your own methods and attrs (just avoid name collisions with the special names)
- Can create class hierarchies
