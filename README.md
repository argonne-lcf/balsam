<p align="center">
<a href="https://balsam.readthedocs.io">
<img align="center" src="docs/_static/logo/small3.png" style="border: 0;">
</a>
</p>

![GitHub tag (latest by date)](https://img.shields.io/github/tag-date/balsam-alcf/balsam.svg?label=version)
[![Documentation Status](https://readthedocs.org/projects/balsam/badge/?version=latest)](https://balsam.readthedocs.io/en/latest/?badge=latest)
![PyPI - License](https://img.shields.io/pypi/l/balsam-flow.svg)

# Balsam: HPC Workflows & Edge Service

Balsam makes it easy to manage large computational campaigns on a
supercomputer. Instead of writing and submitting job scripts to the batch
scheduler, you send individual tasks (application runs) to Balsam. The **service** takes
care of reserving compute resources in response to changing workloads.  The
**launcher** fetches tasks and executes the workflow on its allocated
resources.

Balsam is designed to minimize user "buy-in" and cognitive overhead. You
don't have to learn an API or write any glue code to acheive throughput with
existing applications. On systems with Balsam installed, it's arguably faster
and easier for a beginner to run an ensemble using Balsam than
by writing an ensemble job script:

```console
$ balsam app --name SayHello --executable "echo hello,"
$ for i in {1..10}
> do
>  balsam job --name hi$i --workflow test --application SayHello --args "world $i"
> done
$ balsam submit-launch -A Project -q Queue -t 5 -n 2 --job-mode=serial
```

## Highlights

- Applications require zero modification and run *as-is* with Balsam
- Launch MPI applications or pack several non-MPI tasks-per-node
- Run apps on bare metal or [inside a Singularity container](https://www.alcf.anl.gov/user-guides/singularity)
- Flexible Python API and command-line interfaces for workflow management
- Execution is load balanced and resilient to task faults. Errors are automatically recorded to database for quick lookup and
  debugging of workflows
- Scheduled jobs can overlap in time; launchers cooperatively consume work from the same database
- Multi-user workflow management: collaborators on the same project can add tasks and submit launcher jobs using
  the same database

The Balsam API enables a variety of scenarios beyond the independent bag-of-tasks:
- Add task dependencies to form DAGs
- Program dynamic workflows: some tasks spawn or kill other tasks at runtime
- Remotely submit workflows, track their progress, and coordinate data movement tasks

## **Read the Balsam Documentation online at** [balsam.readthedocs.io](https://balsam.readthedocs.io/en/latest/)!

## Existing site-wide installations

Balsam is deployed in a public location at the following sites.  On these systems,
it's not necessary to install Balsam yourself:

|Location | System | Command|
|---------|--------|-------|
|ALCF     | Theta | `module load balsam` |

## Installation

#### Prerequisites
Balsam requires Python 3.6 or later. Preferably, set up an isolated
virtualenv or conda environment for Balsam. It's no problem if some
applications in your workflow run in different Python environments. You will
need setuptools 39.2 or newer:

```console
$ pip install --upgrade pip setuptools
```

Some Balsam components require [mpi4py](https://github.com/mpi4py/mpi4py),  so
it is best to install Balsam in an environment with `mpi4py` already in place
and configured for your platform.  **At the minimum**, a working MPI
implementation and `mpicc` compiler wrapper should be in the search path, in
which case the `mpi4py` dependency will automatically build and install.

[cython](https://github.com/cython/cython) is also used to compile some
CPU-intensive portions of the Balsam service.  While the Cython dependency will
also be installed if it's absent, it is preferable to have an existing version
built with your platform-tuned compiler wrappers.

Finally, Balsam requires PostgreSQL version 9.6.4 or newer to be installed. You can verify
that PostgreSQL is in the search `PATH` and the version is up-to-date with:

```console
$ pg_ctl --version
```

It's very easy to [get the PostgreSQL binaries](https://www.enterprisedb.com/download-postgresql-binaries) if you
don't already have them.  Simply adding the PostgreSQL `bin/` to your search
PATH should be enough to use Balsam without having to bother a system
administrator.

#### Quick setup

```console
$ pip install balsam-flow
$ balsam init ~/myWorkflow
$ source balsamactivate myWorkflow
```

Once a Balsam database is activated, you can use the command line to manage your workflows:

```console
$ balsam app --name SayHello --executable "echo hello,"
$ balsam job --name hi --workflow test --application SayHello --args "World!"
$ balsam submit-launch -A MyProject -q DebugQueue -t 5 -n 1 --job-mode=mpi
$ watch balsam ls   #  follow status in realtime from command-line
```

## **Keep reading the Balsam Documentation online at** [balsam.readthedocs.io](https://balsam.readthedocs.io/en/latest/)!

## Citing Balsam
If you are referencing Balsam in a publication, please cite the following paper:

-  M. Salim, T. Uram, J.T. Childers, P. Balaprakash, V. Vishwanath, M. Papka. *Balsam: Automated Scheduling and Execution of Dynamic, Data-Intensive HPC Workflows*. In Proceedings of the 8th Workshop on Python for High-Performance and Scientific Computing. ACM Press, 2018.
