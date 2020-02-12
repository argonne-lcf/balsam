Submitting Jobs
===============

The [balsam launcher]{.title-ref} command starts running the launcher
component inside of a scheduled job. The launcher automatically detects
the allocated resources and begins executing your workflows.

However, you typically **do not** run [balsam launcher]{.title-ref}
yourself, unless you are working interactively to test and debug
workflows. From a login node, there are two ways to submit jobs that run
[balsam launcher]{.title-ref} for you:

1.  `balsam submit-launch`{.interpreted-text role="bash"} \-- manually
    submit a job using the specified resources
2.  `balsam service`{.interpreted-text role="bash"} \-- start up the
    **service** daemon, which persists on the login node and submits
    launcher jobs on your behalf over time.

In either case, the following takes place when you submit with Balsam:

-   A bash script is generated from the Job Template (see
    `JobTemplate`{.interpreted-text role="ref"} for an easy way to
    \"hook-in\" custom data movement or other pre-run commands)
-   The script is named `qlaunch<id>.sh`{.interpreted-text role="bash"}
    and saved in the `qsubmit/`{.interpreted-text role="bash"}
    subdirectory of your database
-   The script is submitted to the batch scheduler using the platform\'s
    scheduler interface, via `qsub`{.interpreted-text role="bash"}, for
    instance

Manually Submit with **submit-launch**
--------------------------------------

`balsam submit-launch`{.interpreted-text role="bash"} passes on most of
its arguments to the local scheduler submission command (e.g.
[qsub]{.title-ref}). Hence, usage is straightforward:

``` {.bash}
$ balsam submit-launch -A Project -q Queue -t 15 -n 5 --job-mode=mpi
```

The only required argument that is unique to Balsam is the
**\--job-mode**, explained below.

### MPI job mode

For `--job-mode=mpi`{.interpreted-text role="bash"}, the Balsam launcher
runs as a pilot job on the head node of the allocated resources. From
this head node, it issues MPI launch commands (**mpirun** or equivalent)
to launch jobs against the available resources.

This job mode maps very closely to the traditional \"ensemble job\"
script that you may be accustomed to thinking about and can run any kind
of task, whether it actually uses MPI or not.

::: {.note}
::: {.title}
Note
:::

The launcher continuously attempts to launch tasks on idle nodes. Tasks
are prioritized in order of decreasing **num\_nodes** and then
decreasing **wall\_time\_minutes** (see `BalsamJob`{.interpreted-text
role="ref"}). This means that bigger, longer jobs will start earlier
than smaller, shorter jobs at the beginning. The **wall\_time\_minutes**
is a completely optional field that defaults to 0. If you don\'t want to
provide a runtime estimate, simply leave it blank and jobs will not be
prioritized according to this value.
:::

### Serial job mode

For tasks that run on a single node and **do not** use MPI, you may wish
to pack several tasks per node. You may also wish to run 4000 such tasks
on 4000 nodes, without overburdening the head node with 4000 **mpirun**
background processes.

The [\--job-mode=serial]{.title-ref} option solves both of these
problems by launching a single forker process on each compute node.
These processes then run isolated tasks on the single nodes. This job
mode **will not** process any tasks that have specified the use of
multiple MPI ranks.

### Filtering jobs by workflow tag

By default, launchers will consume **all runnable tasks** from the
database. If you want a launcher job to **only** run tasks with a
specific workflow tag, you can provide the **wf-filter** option:

``` {.bash}
$ balsam submit-launch -A Project -q Queue -t 15 -n 5 --job-mode=mpi --wf-filter=Experiment3
```

Now, only tasks whose `workflow`{.interpreted-text role="bash"} field
contains the substring \"Experiment3\" will be eligible to run inside
this job. This is a useful way to limit what workflows are allowed to
run in which job. Of course, if you are running a large campaign, it is
useful to use the `workflow`{.interpreted-text role="bash"} tag merely
for organization and omit the `--wf-filter`{.interpreted-text
role="bash"} option, so that all jobs can get as much work done as
possible.

### Running many launchers concurrently

One of the biggest advantages of Balsam is that many launcher jobs can
be scheduled to run concurrently.

If you have a lot of tasks to run, simply call
`submit-launch`{.interpreted-text role="bash"} multiple times (up to the
queue limit, if you like) to enqueue many launcher jobs. They may run
one-after-another or simultaneously, depending on resource availability.
When launchers run simultaneously, they cooperatively \"check-out\" idle
runnable tasks from the database, ensuring that every task runs exactly
one time and there is no duplicate processing of tasks.

Allowing Balsam Service to Submit Jobs
--------------------------------------

Running `balsam service`{.interpreted-text role="bash"} starts a daemon
on the login node tho automatically package and reserve groups tasks for
job submission. **This is the cornerstone of using Balsam as an edge
service:** when the service is on, remotely-submitted jobs can be
processed without ever having to log in yourself.

Monitoring and Killing Jobs
---------------------------

You can always use the local scheduler\'s utilities to monitor (e.g.
`qstat`{.interpreted-text role="bash"}) and kill (e.g.
`qdel`{.interpreted-text role="bash"}) Balsam jobs. Running launchers
will intercept [SIGINT]{.title-ref} or [SIGTERM]{.title-ref} signals
sent by the scheduler when a job is killed or timed out. In turn, they
gracefully stop running your workflow and mark timed-out tasks
accordingly.

Note: for Slurm, use
`scancel --batch --signal=TERM job_id`{.interpreted-text role="bash"}
(or `INT`{.interpreted-text role="bash"}) to kill a running Balsam
launcher and allow it to gracefully exit. Unlike the Cobalt scheduler,
Slurm signals the child processes of the batch shell process by default
when canceling jobs, including the job steps invoked by
[srun]{.title-ref}. The `--batch`{.interpreted-text role="bash"} option
prevents this behavior, and the `--signal`{.interpreted-text
role="bash"} option overrides the default sending of
`SIGKILL`{.interpreted-text role="bash"} to the batch step.

If you want to kill a **particular task** while it\'s running inside a
launcher, you can use the `balsam kill <jobid>`{.interpreted-text
role="bash"} command (see `BalsamCLI`{.interpreted-text role="ref"}).
The killed task will stop in near-realtime and be replaced by the next
eligible task to run.
