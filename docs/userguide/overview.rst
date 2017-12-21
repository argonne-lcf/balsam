Overview
========

Why do I want this?
--------------------
Whereas a local batch scheduler like Cobalt runs on behalf of **all users**,
with the goals of fair resource sharing and maximizing overall utilization,
Balsam runs on **your** behalf, interacting with the scheduler to check for
idle resources and sizing jobs to minimize time-to-solution.

You could use Balsam as a drop-in replacement for ``qsub``, simply using
``balsam qsub`` to submit your jobs with absolutely no restrictions. Let Balsam
throttle submission to the local queues, package jobs into ensembles for you,
and dynamically size these packages to exploit local scheduling policies.

Simple jobs or complex workflows
--------------------------------
You can also use Balsam to author arbitrarily complex workflows; organize jobs
into a directed acyclic graphs (DAGs) representing the flow of data from parent
to child jobs. Balsam will execute the workflows on your behalf, ensuring the
correct order of jobs and flow of data. Balsam will maximize concurrency by
packaging your serial (non-MPI) jobs into MPI ensembles, and running several
MPI jobs concurrently.

You can take advantage of a simple Python API to express dynamic workflows that
create and destroy jobs according to the results of some application, or create
"rescue" jobs in response to runtime errors or walltime-exceeded scenarios. A
rich command line interface allows you to manipulate the Balsam database
interactively or write job processing scripts in any other programming
language.

Automatic Meta-scheduling
--------------------------
While you can immediately benefit from using ``balsam qsub``, Balsam asks that
you take some time to decompose your workflow into Applications that have a
predictable runtime as a function of some pre-specified inputs. This means that
one *executable* might be split into three Balsam Applications, each
representing a different functionality.

In return, Balsam will automatically time your jobs and use this timing data to
train performance models of the applications. These performance models will
take parallel efficiency into account, and will provide a huge benefit in
high-throughput computing (HTC) scenarios, where the same application is
running on the same hardware with many different inputs. 

Balsam will use these models to assign node and walltime requirements for the
jobs, constrained by available resources and minimum acceptable parallel
efficiencies.  This accurate and fully-automatic assignment of resources to
jobs goes hand-in-hand with the meta-scheduling, which will package tasks and
submit them to the queues in a fashion that minimizes overall time to solution.

Workflows across computers
---------------------------
Balsam comes packaged with the **Argo** manager, which will provide a
synchronization mechanism for workflows (with arbitrary data dependencies)
spanning multiple remote sites. For jobs that can run on more than one
resource, Argo will optimize the scheduling of jobs by communicating with each
Balsam instance.

Jobs can be automatically generated in response to some event (e.g. filesystem
change) at the Argo site or one of the Balsam sites, and these new jobs are
synchronized to the Balsam sites at which they will actually run.  Data flow
between jobs at different locations is handled transparently.  Finally, Argo
provides a web service for monitoring the execution of workflows across Balsam
instances.
