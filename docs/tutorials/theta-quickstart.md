# Getting started

This tutorial gets you up and running with a new Balsam Site quickly.  Since
Balsam is highly platform-agnostic, you can follow along by choosing from any of
the available default site setups:

- A local MacOS or Linux system
- Theta-KNL
- Theta-GPU
- Cooley
- Cori (Haswell or KNL partitions)
- Perlmutter
- Summit

## Install

First create a new virtualenv and install Balsam:

```bash
$ /soft/datascience/create_env.sh my-env # Or DIY
$ source my-env/bin/activate
$ pip install --pre balsam
```

## Log In 

Now that `balsam` is installed, you need to log in. 
Logging in fetches an access token that is used to identify you 
in all future API interactions, until the token expires and you have to log in again.

```bash
$ balsam login
# Follow the prompt to authenticate
```

Once you are logged in, you can create a Balsam Site for job execution, or send jobs to any of your other existing Sites.

!!! warning "Login temporarily restricted"
    Balsam is currently in a pre-release stage and the web service is hosted on limited resources.  Consequently, logins are limited to pre-authorized users.  Please contact the [ALCF Help Desk](mailto:support@alcf.anl.gov) to request early access membership to the Balsam user group. 



## Create a Balsam Site

All Balsam workflows are namespaced under **Sites**: self-contained project
spaces belonging to individual users. You can use Balsam to manage
Sites on multiple HPC systems from a single shell or Python program.
This is one of the key strengths of Balsam: the usage looks exactly the same
whether you're running locally or managing jobs across multiple supercomputers.

Let's start by creating a Balsam Site in a folder named `./my-site`:

```bash
$ balsam site init ./my-site
# Select the default configuration for Theta-KNL
```

You will be prompted to select a default Site configuration and to enter a
**unique name** for your Site.  The directory `my-site/` will then be created
and preconfigured for the chosen platform.  You can list your Sites with the `balsam
site ls` command.

```bash
$ balsam site ls

   ID             Name   Path                               Active
   21       theta-demo   .../projects/datascience/my-site   No 
```

In order to actually run anything at the Site, **you have to enter the Site
directory and start it** with `balsam site start`.  This command launches
a persistent background agent which uses your access token to sync with the
Balsam service.

```bash
$ cd my-site
$ balsam site start
```


## Set up your Apps

Every Site has its own collection of Balsam **Apps**, which define the runnable
applications at that Site.   A Balsam App is declared by writing an `ApplicationDefinition`
class and running the `sync()` method from a Python program or interactive session.
The simplest `ApplicationDefinition` is just a template for a `bash`
command, with any workflow variables enclosed in double-curly braces:

```python
from balsam.api import ApplicationDefinition

class Hello(ApplicationDefinition):
    site = "theta-demo"
    command_template = "echo Hello, {{ say_hello_to }}!"

Hello.sync()
```

Notice the attribute `site = "theta-demo"` which is **required** to associate the App `"Hello"` to the Site `"theta-demo"`. 

In addition to shell command templates, we can define Apps that invoke a Python `run()`
function on a compute node:

```python
class VecNorm(ApplicationDefinition):
    site = "theta-demo"

    def run(self, vec):
        return sum(x**2 for x in vec)**0.5

VecNorm.sync()
```

After running the `sync()` methods for these Apps, they are serialized and
shipped into the Balsam cloud service.  We can then load and re-use these Apps when
submitting Jobs from other Python programs.


## Add Jobs

With these App classes in hand, we can now submit some jobs from the Python SDK.
Let's create a Job for both the `Hello` and `VecNorm` apps:  all we need is to 
pass a **working directory** and any necessary **parameters** for each:

```python
hello = Hello.submit(workdir="demo/hello", say_hello_to="world")
norm = VecNorm.submit(workdir="demo/norm", vec=[3, 4])
```

Notice how shell command parameters (for `Hello`) and Python function parameters
(for `VecNorm`) are treated on the same footing.  We have now created two Jobs that will eventually run on the Site `theta-demo`, once compute resources are available.  These Jobs can be seen by running `balsam job ls`.


## Make it run

A key concept in Balsam is that **Jobs** only specify *what* to run, and you must
create **BatchJobs** to provision the resources that actually *execute* your jobs.
This way, your workflow definitions are neatly separated from the
concern of what allocation they run on.  You create a collection of Jobs first,
and then many of these Jobs can run inside one (or more) BatchJobs.

Since BatchJobs dynamically acquire Jobs, Balsam execution is fully **elastic** (just spin up more nodes by adding another BatchJob) and **migratable** (a Job that ran out of time in one batch allocation will get picked up in the next BatchJob).  BatchJobs will *automatically* run as many Jobs as they can at their Site.  You can simply
queue up one or several BatchJobs and let them divide and conquer your workload. 

```python
BatchJob.objects.create(
    site_id=hello_job.site_id,
    num_nodes=1,
    wall_time_min=10,
    job_mode="mpi",
    queue="local",  # Use the appropriate batch queue, or `local`
    project="local",  # Use the appropriate allocation, or `local`
)
```

After running this command, the Site agent will fetch the new BatchJob and
perform the necessary pilot job submission with the local resource manager (e.g.
via `qsub`).  The `hello` and `norm` Jobs will run in the `workdirs` specified
above, located relative to the Site's `data/` directory.  You will find the
"Hello world" job output in a `job.out` file therein.  

For Python `run()` applications, the created `Jobs` can be handled like
[`concurrent.futures.Future`](https://docs.python.org/3/library/concurrent.futures.html#future-objects)
instances, where the `result()` method delivers the return value (or re-raises
the Exception) from a `run()` invocation.

```python
assert norm.result() == 5.0  # (3**2 + 4**2)**0.5
```

When creating BatchJobs, you can verify that the allocation was actually created
by checking with the local resource manager (e.g.  `qstat`) or by checking with
Balsam:

```bash
$ balsam queue ls
```

When the BatchJob with `job_mode="mpi"` starts, an [MPI mode launcher](../../user-guide/batchjob#selecting-a-launcher-job-mode) pilot job acquires and runs the jobs. You will find helpful logs in the `logs/`  directory showing what's going on.  Follow the Job statuses with `balsam job ls`:

```
$ balsam job ls

ID       Site                  App          Workdir   State          Tags  
267280   thetalogin4:my-site   Hello        test/2    JOB_FINISHED   {}    
267279   thetalogin4:my-site   Hello        test/1    JOB_FINISHED   {} 
```



## A complete Python example

Now let's combine the Python snippets from above to show a
self-contained example of Balsam SDK usage (e.g. something
you might run from a Jupyter notebook):

```python
from balsam.api import ApplicationDefinition, BatchJob, Job

class VecNorm(ApplicationDefinition):
    site = "my-local-site"

    def run(self, vec):
        return sum(x**2 for x in vec)**0.5

jobs = [
    VecNorm.submit(workdir="test/1", vec=[3, 4]),
    VecNorm.submit(workdir="test/2", vec=[6, 8]),
]

BatchJob.objects.create(
    site_id=jobs[0].site_id,
    num_nodes=1,
    wall_time_min=10,
    job_mode="mpi",
    queue="local",
    project="local",
)

for job in Job.objects.as_completed(jobs):
    print(job.workdir, job.result())
```

## Submitting Jobs from the command line

You can check the Apps registered at a given Site, or across
all Sites, from the command line:

```bash
$ balsam app ls --site=all

   ID                 Name   Site
  286                Hello   laptop
  287              VecNorm   laptop
```

To create a Balsam Job from the CLI, you must identify the App on the command
line:

```bash
$ balsam job create --site=laptop --app Hello --workdir demo/hello2 --param say_hello_to="world2" 
```

There are many additional CLI options in job creation, which can be summarized with `balsam job create --help`.  You will usually create jobs using Python, but the CLI remains useful for monitoring workflow status:

```
$ balsam job ls

ID       Site     App        Workdir          State          Tags
501649   laptop   Hello      test/1           PREPROCESSED   {}
501650   laptop   Hello      test/2           STAGED_IN      {}
```

BatchJobs can be submitted from the CLI, with parameters that mimic a standard scheduler interface:
```bash
# Substitute -q QUEUE and -A ALLOCATION for your project:
$ balsam queue submit -q debug-cache-quad -A datascience -n 1 -t 10 -j mpi 
```