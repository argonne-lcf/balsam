Command Line Interface
=======================

On the previous page, we created a new Balsam database with `balsam init` and
activated the database with `source balsamactivate`.
Now, let's explore the command line interface, which is frequently
used to interact with your database and submit new jobs.

## Checking which database is active

We can confirm which database is currently in use with `balsam which`:
```bash
# Confirm that you have a test DB activated:
$ balsam which

Current Balsam DB: /path/to/database
{'host': 'thetalogin4', 'port': 35872}
```

## Defining new Applications

Let's register a new executable with Balsam.  Our application will consist of the shell command
`echo hello, {args}`, where `{args}` is set individually for each run. We define the new Balsam app with the `balsam app` command like this:

```bash
# Define an app:
$ balsam app --name hello --executable "echo hello, "

Application 1:
-----------------------
name:                           hello
description:
executable:                     echo hello,
preprocess:
envscript:
postprocess:
Added app to database
```

The `--name` argument gives a unique alias for the application, and the `--executable` refers to the first half of the command line (minus the variable `{args}`).

Once an application is defined, it persists in your Balsam database.  You can list your applications in a compact tabular form:

```bash
$ balsam ls apps

pk |  name |  executable | description
--------------------------------------
1  | hello | echo hello, |
```

or in a verbose layout showing all Application fields:
```bash
$ balsam ls apps --verbose

Application 1:
-----------------------
name:                           hello
description:
executable:                     echo hello,
preprocess:
envscript:
postprocess:
```

The empty fields `description`, `preprocess`, `envscript`, and `postprocess` are optional here and will be covered in-depth in [a later tutorial](nwchem.md).

## Adding Application Runs (aka BalsamJobs)

Now we want to run this application several times.  We define two runs with the `balsam job` command as follows:

```bash
# Add a couple instances of the hello app:
$ balsam job --name hello-world --workflow demo-hello   --app hello --args 'world!'
$ balsam job --name hello-workshop   --workflow demo-hello   --app hello --args 'workshop!' --ranks-per-node 2
```

The `balsam job` command constructs an instance of the `hello` app that we just defined.
We specify how the run should look with the following fields:
 
  - `--name`:  A string to identify each run
  - `--workflow`:  A string to identify a group of related runs.  Taken together, the
  `(name, workflow)` pair should be a unique identifier.
  - `--app`: A reference to the Balsam Application to execute.  In this case, we use the `hello` app that we just defined.
  - `--args`: Command-line arguments to the `hello` executable
  - `--ranks-per-node`: Number of MPI ranks per node to launch the application with. In this case the application `echo` is obviously not using MPI, so we'll end up running two duplicate processes on the same compute node and see the output repeated twice.

The `args` are joined with the application `executable` by string concatentation.  
For the second job named `hello-workshop`, we therefore execute `"echo hello, "` + `"workshop!"`, which results in `echo hello, workshop!`

Without the `--yes` argument, you should see a detailed confirmation listing all details of 
the created BalsamJob:

```bash
BalsamJob d27257cb-925a-4818-97ef-a513db58bce4
----------------------------------------------
workflow:                       demo-hello
name:                           hello-world
description:
lock:
parents:                        []
input_files:                    *
stage_in_url:
stage_out_files:
stage_out_url:
wall_time_minutes:              1
num_nodes:                      1
coschedule_num_nodes:           0
ranks_per_node:                 1
cpu_affinity:                   none
threads_per_rank:               1
threads_per_core:               1
node_packing_count:             1
environ_vars:
application:                    hello
args:                           world!
user_workdir:
wait_for_parents:               True
post_error_handler:             False
post_timeout_handler:           False
auto_timeout_retry:             True
state:                          CREATED
queued_launch_id:               None
data:                           {}
  *** Executed command:         echo hello, world!
  *** Working directory:        /path/to/testdb/data/demo-hello/hello-world_d27257cb

Confirm adding job to DB [y/n]: y
```

These fields control every aspect of how this instance of your Application (BalsamJob) will
run. Creating the BalsamJob does not run anything in itself, you are just telling the system *what* to run at a later date.  One advantage of this approach is that you can populate the database with thousands or millions of jobs, and allow them to run over several Cobalt batch jobs.  

Most BalsamJob fields are omitted in this simple example and default to sensible values. For more information on BalsamJob fields, refer to the [Guide to defining applications](../userguide/app.md#balsamjob-fields).

You will notice that a working directory has been chosen for you.  Balsam associates each
job with a unique working directory, named according to the following convention:
```python
f"data/{job.workflow}/{job.name}_{job.id[:8]}"
```
That is, the `workflow` has a greater signifance in grouping related runs into a shared 
working directory.

## List your BalsamJobs

`balsam ls` is by far the most frequently used command to list the status of all your jobs.
It is actually shorthand for `balsam ls jobs`:

```bash
$ balsam ls
                              job_id |        name |   workflow | application |   state
---------------------------------------------------------------------------------------
d27257cb-925a-4818-97ef-a513db58bce4 | hello-world      | demo-hello | hello       | CREATED
391a92cf-0a65-4341-a6ac-83dbfe12b844 | hello-workshop   | demo-hello | hello       | CREATED

$ balsam ls jobs
                              job_id |        name |   workflow | application |   state
---------------------------------------------------------------------------------------
d27257cb-925a-4818-97ef-a513db58bce4 | hello-world      | demo-hello | hello       | CREATED
391a92cf-0a65-4341-a6ac-83dbfe12b844 | hello-workshop   | demo-hello | hello       | CREATED
```

You can set the environment variable `BALSAM_LS_FIELDS` to add columns to this view:
```bash
$ BALSAM_LS_FIELDS=ranks_per_node:args balsam ls
                              job_id |        name |   workflow | application |   state | ranks_per_node |          args
------------------------------------------------------------------------------------------------------------------------
d27257cb-925a-4818-97ef-a513db58bce4 | hello-world      | demo-hello | hello       | CREATED | 1              | world!
391a92cf-0a65-4341-a6ac-83dbfe12b844 | hello-workshop   | demo-hello | hello       | CREATED | 2              | workshop!
```

The important column to note here is the `state` which is `CREATED` for all jobs. 
This means that absolutely *nothing* has happened yet: the runs are just registered 
in the database and waiting for a Launcher job to be submitted to the queues.

## Launch your work through the job queue

Now we submit a **Balsam launcher** job through Cobalt to actually run our jobs.  When the job starts running, the launcher will pull our two `hello` tasks from the database and run them.
```bash
# Now submit a job to run those tasks
# Important: Please modify the project (-A) and (-q) as necessary for your allocation/machine:
balsam submit-launch -q Comp_Perf_Workshop -A Comp_Perf_Workshop -n 1 -t 5 --job-mode mpi
```

Here, we have requested a 1 node job for 5 minutes.  The `mpi` job mode indicates that the regular 
system job launch command (i.e. `aprun` on Theta) will be used to invoke our applications.

We can use `watch balsam ls` to refresh the output of `balsam ls` every 2 seconds. Eventually, we should
see the applications advance to state `RUNNING` and eventually `JOB_FINISHED`:

```bash
$ watch balsam ls

                              job_id |        name |   workflow | application |        state
--------------------------------------------------------------------------------------------
d27257cb-925a-4818-97ef-a513db58bce4 | hello-world      | demo-hello | hello       | JOB_FINISHED
391a92cf-0a65-4341-a6ac-83dbfe12b844 | hello-workshop   | demo-hello | hello       | JOB_FINISHED
```

To jump into the working directory of a job, we can use `. bcd {first-few-chars-of-job-id}`. We should
see the output of our `echo helo, world!` job in the `.out` file that's created therein:

```bash
$ . bcd d272  # first few characters of hello-world job
$ cat hello-world.out

hello, world!
Application 20838873 resources: utime ~0s, stime ~1s, Rss ~6292, inblocks ~8, outblocks ~0
```
