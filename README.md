# Balsam: HPC Workflows & Edge Service

<p align="center">
<img src="docs/_static/logo/small3.png">
</p>

![GitHub tag (latest by date)](https://img.shields.io/github/tag-date/balsam-alcf/balsam.svg?label=version)
[![Documentation Status](https://readthedocs.org/projects/balsam/badge/?version=latest)](https://balsam.readthedocs.io/en/latest/?badge=latest)
![PyPI - License](https://img.shields.io/pypi/l/deephyper.svg)
![PyPI - Downloads](https://img.shields.io/pypi/dm/deephyper.svg?label=Pypi%20downloads)

**Read the Balsam Documentation online at** [balsam.readthedocs.io](https://balsam.readthedocs.io/en/latest/)!

## Installation

#### Prerequisites
Balsam requires Python 3.6 or later.  You will need setuptools 39.2 or newer:

```console
$ pip install --upgrade pip setuptools
```

Some Balsam components require `mpi4py`, so it is best to install Balsam in an
environment with [mpi4py](https://github.com/mpi4py/mpi4py) already in place
and configured for your platform.  At the very least, a working MPI
implementation and `mpicc` compiler wrapper should be in the search path so
that the dependency can be automatically installed.

Cython is also used to compile some CPU-intensive portions of the Balsam
service.  While the Cython dependency will also be installed if it's absent, it
is preferable to have an existing version built with your platform-tuned compiler wrappers.


#### Quick setup

```console
$ pip insall Balsam   # Capital B!
```

