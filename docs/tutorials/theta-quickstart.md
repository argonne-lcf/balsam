# Getting started

This tutorial gets you up and running with a new Balsam Site quickly.
Although the examples refer to the Theta-KNL system a few times, Balsam is
highly platform-agnostic, and you can follow along easily on any of the currently
supported systems:

- Theta-KNL
- Theta-GPU
- Cooley
- Cori (Haswell or KNL partitions)
- Summit
- A local MacOS or Linux system

## Install

First create a new virtualenv and install Balsam:

```bash
$ /soft/datascience/create_env.sh my-env # Or DIY
$ source my-env/bin/activate
$ pip install --pre balsam-flow
```

## Log In 

Now that the `balsam` command line tool is installed, you need to log in. 
Logging in fetches an access token that is used to identify you 
in all future API interactions, until the token expires and you have to log in again.

```bash
$ balsam login https://balsam-dev.alcf.anl.gov
# Follow the prompt to authenticate
```

Once you are logged in, you can create a Balsam Site for job execution, or send jobs to any of your other existing Sites.


## Create a Balsam Site

All Balsam workflows are namespaced under **Sites**: self-contained project
spaces belonging to individual users. You can use Balsam to manage
Sites across one or several HPC systems from a single command line interface (or Python script).

Let's start by creating a Balsam site in a folder named `./my-site`:

```bash
$ balsam site init ./my-site
# Select the default configuration for Theta-KNL
```

The directory `my-site/` will be created and preconfigured for Theta-KNL.
You can always check the `balsam site ls` command to list your Sites.

```bash
$ balsam site ls

   ID         Hostname   Path                               Active
   21      thetalogin4   .../projects/datascience/my-site   No 
```

## Set up your Apps

To run an application in Balsam, you must submit a **Job** that refers to a
specific **App** registered at one of your Sites.  Every Site has its own
colllection of Balsam Apps. A Balsam App is simply declared by writing an `ApplicationDefinition`
class in a Python file located in the `apps/` folder inside your Site.

The simplest `ApplicationDefinition` is just a declaration of the application command line, with any adjustable parameters enclosed in double-curly braces:

```python
from balsam.site import ApplicationDefinition

class Hello(ApplicationDefinition):
    command_template = "echo Hello, {{ name }}!"
```

In fact, you'll find this "Hello, world" example already written in the `apps/demo.py` file of your new Site!   Feel free to add more `ApplicationDefinitions` to `demo.py`, or create additional Python files with their own `ApplicationDefinitions`.  Whenever your Apps change, just run:

```bash
$ balsam app sync
```

Your can use the CLI to get a listing of all Apps defined across all your Sites:

```bash
$ balsam app ls --site=all

   ID            ClassPath   Site                
   56           demo.Hello   thetalogin4:.../projects/datascience/my-site
```


## Add Jobs

To create a Balsam Job from the CLI, you must provide the app name and working directory,
along with any parameters required by the command template:
```bash
$ balsam job create --app demo.Hello --workdir test/1 --param name="world"
```

There are many additional CLI options in job creation, which can be summarized with `balsam job create --help`.
More often, when you need to create a large number of related Jobs, the Python API is 
the best option:

```python
from balsam.api import Job
 
for i in range(10):
    Job.objects.create(
        site_path="my-site",
        app_name="demo.Hello",
        workdir=f"test-api/{i}", 
        parameters={"name": f"world {i}"},
        node_packing_count=10,
    )
```

Your jobs can be viewed from the CLI:
```
$ balsam job ls

ID       Site                  App          Workdir   State       Tags  
267280   thetalogin4:my-site   demo.Hello   test/2    STAGED_IN   {}    
267279   thetalogin4:my-site   demo.Hello   test/1    STAGED_IN   {} 
```

## Running the Site

Thus far, we've only interacted with the Balsam web service via the CLI or Python.
So how does anything actually run on the HPC system?
To start, **you have to turn the Site on**:

```bash
$ cd my-site
$ balsam site start
```

With this command, the Site runs an agent on your behalf.  The agent runs
persistently in the background, using
your access token to sync with the
Balsam backend and orchestrate workflows locally. 

You may need to restart the Site when the system is rebooted or otherwise goes down
for maintenance.  You can always stop the Site yourself:

```bash
$ balsam site stop
```

## Queueing a BatchJob

A key concept in Balsam is that **Jobs** only specify *what* to run, and you must
create **BatchJobs** to provision the resources that actually *execute* your jobs.

This way, your workflow definitions are neatly separated from the
concern of what allocation they run on.  You create a collection of Jobs first,
and then many of these Jobs can run inside one (or more) BatchJobs.

BatchJobs will *automatically* run as many Jobs as they can at their Site.  You can simply
queue up one or several BatchJobs and let them divide and conquer your workload. 

```bash
# Substitute -q QUEUE and -A ALLOCATION for your project:
$ balsam queue submit -q debug-cache-quad -A datascience -n 1 -t 10 -j mpi 
```

After running this command, the Site agent will soon detect the new BatchJob, and 
*synchronize* the local state by making a pilot job submission to the local resource manager (e.g. via `qsub`). 
You can verify that the allocation was actually created by checking with the local resource manager (e.g.  `qstat`) or by checking with Balsam:

```bash
$ balsam queue ls
```

When the BatchJob starts, an MPI mode launcher pilot job will run your jobs. You will see (copious) logs in the `logs/`  directory showing what's going on.  Follow the Job statuses with `balsam job ls`:

```
$ balsam job ls

ID       Site                  App          Workdir   State          Tags  
267280   thetalogin4:my-site   demo.Hello   test/2    JOB_FINISHED   {}    
267279   thetalogin4:my-site   demo.Hello   test/1    JOB_FINISHED   {} 
```

Each job runs in the `workdir` that we specified, located relative to the Site's
`data/` directory.  You will find the "Hello world" job outputs in `job.out`
files therein.
