Balsam Python API
==================

In this example, we'll look at a more effective way to create BalsamJobs
with the Python API provided by Balsam.  We will also work with an application that
is itself written in Python and accesses the Balsam database to receive its input and
write its output.

We will write a Python script that serves three purposes:

  1. A function representing the application itself: to `square` a number
  2. To register the applcation in the Balsam database
  3. To populate the database with `N` jobs of the `square` app

## The data models

Balsam is built on the [Django
ORM](https://docs.djangoproject.com/en/3.0/topics/db/queries/) which provides a
Python object-oriented model of the underlying database tables.  In the last
example, we created BalsamJobs with the `balsam job` shell command and defined
applications with the `balsam app` command.  We can also manipulate these tables
by importing the `BalsamJob` and `ApplicationDefinition` classes:

```python
from balsam.core.models import BalsamJob, ApplicationDefinition
```

## Defining an Application

Our script can bootstrap the `ApplicationDefinition` if it doesn't already exist:

```python
import os

# Bootstrap app if it's not already in the DB
app, was_created = ApplicationDefinition.objects.get_or_create(
    name = 'square',
)

if was_created:
	app.executable = os.path.abspath(__file__)
	app.save()
```
The location of this script may resolve to different filepaths depending on where the
script is run on HPC systems, e.g. on a compute node vs. a login node. Each application
must have a unique name in the Balsam database; therefore, we only define the `executable`
attribute the first time the script is run and the app is created.

Since the script creating the app is itself serving as the app `executable`,
we set the fully qualified path as `os.path.abspath(__file__)`. 

!!! note
    When setting a Python script directly as the `executable`, remember that the 
    permission  bit must also be set with `chmod +x` and that the first line of the 
    script contains the interpreter (e.g. `#!/usr/bin/env python`).

## The Application code

A running Python script can access the context of job currently running in Balsam by
importing it from: `balsam.launcher.dag.current_job`. This is useful when the 
code needs to access database state, such as the `data` field on the job providing
JSONB storage.

Let's write a `run` function that:

  1. Takes a BalsamJob `job` in as context
  2. Squares the number stored in `job.data["x"]`
  3. Writes the result back to `job.data["y"]`

The code looks like this:

```python
def run(job):
    """If we're inside a Balsam task, do the calculation"""
    x = job.data['x']
    y = x**2
    job.data['y'] = y
    job.save()
```

## Populating the database with jobs

Finally, we need a way to add jobs to the database. The command line `balsam job` is
useable but often less convenient for populating a large number of jobs at once.  It's
much nicer to do it programatically:

```python
def create_jobs(N):
    """Invoked outisde a BalsamJob context: create N tasks to square a number"""
    for i in range(N):
        job = BalsamJob(
            name = f"square{i}",
            workflow = "demo-square",
            application = "square",
        )
        job.data["x"] = i
        job.save()
    print(f"Created {N} jobs")
```

Here we create `N` instances of the `square` app, each with a different input value for
`x` set on the `job.data` field. Notice how `name`, `workflow`, and `application` are set
exactly as with the command line arguments to `balsam job` in the previous example.


## The entry point
We need an entry point to distinguish whether to invoke `create_jobs(N)` to populate 
the database with new runs, or whether to invoke `run(job)` inside of a running BalsamJob.
We can easily do this by checking whether the `current_job` object is `None`:

```python
# Entry point
if __name__ == "__main__":
    if current_job:
        run(current_job)
    else:
        N = int(sys.argv[1])
        create_jobs(N)
```

## Running the example
```bash
# Add 10 tasks with the script:
python app.py 10

# View the tasks:
BALSAM_LS_FIELDS=data balsam ls

# Now submit a job to run those tasks
# Important: Please modify the project (-A) and (-q) as necessary for your allocation/machine:
balsam submit-launch -n 2 -q Comp_Perf_Workshop -A Comp_Perf_Workshop -t 5 --job-mode mpi

# Use `watch balsam ls` to track the status of each task in your DB
BALSAM_LS_FIELDS=data  watch balsam ls
```

## The full code example
```python
#!/usr/bin/env python
import os
import sys
from balsam.core.models import BalsamJob, ApplicationDefinition
from balsam.launcher.dag import current_job

# Bootstrap app if it's not already in the DB
ApplicationDefinition.objects.get_or_create(
    name = 'square',
    executable = os.path.abspath(__file__),
)

def run(job):
    """If we're inside a Balsam task, do the calculation"""
    x = job.data['x']
    y = x**2
    job.data['y'] = y
    job.save()

def create_jobs(N):
    """If we're on a command line, create N tasks to square a number"""
    for i in range(N):
        job = BalsamJob(
            name = f"square{i}",
            workflow = "demo-square",
            application = "square",
        )
        job.data["x"] = i
        job.save()
    print(f"Created {N} jobs")

if __name__ == "__main__":
    if current_job:
        run(current_job)
    else:
        N = int(sys.argv[1])
        create_jobs(N)
```
