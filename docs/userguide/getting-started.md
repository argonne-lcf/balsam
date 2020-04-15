Getting Started
===============

Site Guides
-----------

The following sections contain quick instructions for getting started on
specific machines.

### Theta (@ALCF)

The fastest way to get Balsam on Theta is to load the module.

```console
$ module load balsam
$ which balsam
/soft/datascience/Balsam/0.3.5.1/env/bin/balsam
$ which python
/soft/datascience/Balsam/0.3.5.1/env/bin/python
```

As you can see, the module loads a Python 3.6 environment with
pre-configured Balsam installation. Don't worry if your applications
rely on different environments or Python installations. You can simply
use the Balsam Python environment to set up and manage your workflows,
while a different version of Python runs in the backend.

### Cooley (@ALCF)

```console
$ source /soft/datascience/balsam/setup.sh
$ which balsam
/soft/datascience/balsam/env/bin/balsam
```

Installation
------------

### Prerequisites

**Balsam requires Python 3.6 or later**. Preferably, set up an isolated
virtualenv or conda environment for Balsam. It's no problem if some
applications in your workflow run in different Python environments. You
will need setuptools 39.2 or newer:

```console
$ pip install --upgrade pip setuptools
```

Some Balsam components require
[mpi4py](https://github.com/mpi4py/mpi4py), so it is best to install
Balsam in an environment with mpi4py already in place. 
**At the minimum**, a working MPI
implementation and `mpicc` compiler wrapper should be in the
search path, in which case `pip install balsam-flow` will automatically build 
`mpi4py`. 

Finally, Balsam requires PostgreSQL version 9.6.4 or newer to be
installed. You can verify that PostgreSQL is in the search
`PATH` and the version is up-to-date with:

```console
$ pg_ctl --version
```

It's very easy to get the [PostgreSQL
binaries](https://www.enterprisedb.com/download-postgresql-binaries) if
you don't already have them. Simply adding the PostgreSQL
`bin/` to your search `PATH` should be enough to use Balsam
without having to bother a system administrator.

### Installation from PyPI

```console
$ pip install balsam-flow
```

### Create a Balsam DB to start working

You can use the command line to start working with Balsam. `balsam
init` is used to create a new database at the specified
path. `source balsmactivate` takes a database path (or
unique substring) and starts up the database.

```console
$ balsam init ~/myWorkflow
$ source balsamactivate myWorkflow
$ balsam app --name SayHello --executable "echo hello,"
$ balsam job --name hi --workflow test --application SayHello --args "World!"
$ balsam submit-launch -A MyProject -q DebugQueue -t 5 -n 1 --job-mode=mpi
$ watch balsam ls   #  follow status in realtime from command-line
```

!!! note
    For production workflows, be sure to create a database on the
    appropriate filesystem for your platform. Typically, your home directory
    is not the right place for production I/O!
