![logo](./img/small3.png)

*This page is for the Balsam 0.6 pre-release. Click [here for stable Balsam 0.5 docs.](https://balsam.readthedocs.io/en/master)*

A unified platform to manage high-throughput workflows across the HPC landscape.

Create Balsam Sites on any laptop, cluster, or supercomputer. 
The Balsam service provides a central API for you to submit tasks to these sites from anywhere.

```python
from balsam.api import App, Job, BatchJob

# Add 10 fibonacci tasks
fibo_app = App.objects.get(class_path="demo.fibonacci")
jobs = [
    Job(
        app_id=fibo_app.id,
        workdir=f"demo-sweep/{n}",
        parameters={"N": n},
    )
    for n in range(10)
]
Job.objects.bulk_create(jobs)

# Request 2 nodes for 10 minutes at the corresponding Site
BatchJob.objects.create(
    site_id=fibo_app.site_id,
    num_nodes=2,
    wall_time_min=10,
    job_mode="serial",
    project="datascience",
    queue="debug-cache-quad",
)

```

## Features

* Simple `pip` installation on any machine with internet access
* Control apps as Python classes: flexible environments and lifecycle hooks
* Distributed by default: submit and monitor tasks securely from *anywhere*
* Define data dependencies for any task: Balsam orchestrates the necessary data transfers
* High-throughput and fault-tolerant task execution on diverse resources
* Elastic queueing: auto-scale resources to the workload size
* Monitoring APIs: query recent task failures, node utilization, or throughput
* Portable: ready to use on diverse platforms (Theta, Summit, Cori, ...) and easily adaptable

## User installation

Balsam users should simply add Balsam to their environment with `pip`. 
```
git clone https://github.com/balsam-alcf/balsam.git
cd balsam
git checkout develop

# Set up Python3.7+ environment
python3.8 -m venv env
source env/bin/activate

# Install with flexible (unpinned) dependencies:
pip install -e .
```

## Installation on Summit

The `cryptography` sub-dependency of `globus-cli` can be troublesome on non-x86 environments, where 
`pip` will try to build from source.  One workaround is to create a conda environment with the 
`cryptography` dependency pre-satisfied, from which `pip install` works smoothly:

```
module load gcc/10.2.0
module load python/3.7.0-anaconda3-5.3.0

conda init bash
source ~/.bashrc
conda create -p ./b2env "cryptography>=1.8.1,<3.4.0" -y
conda activate ./b2env

git clone https://github.com/argonne-lcf/balsam.git
cd balsam
git checkout develop

which python
python -m pip install --upgrade pip
python -m pip install -e .
```

## Developer/server-side installation

For Balsam development and server deployments, there are some additional
requirements.  Use `make install-dev` to install Balsam with the necessary dependencies.  Direct server dependencies (e.g. FastAPI) are pinned to help with reproducible deployments.

```
git clone https://github.com/balsam-alcf/balsam.git
cd balsam
git checkout develop

# Set up Python3.7+ environment
python3.8 -m venv env
source env/bin/activate

# Install with pinned deployment and dev dependencies:
make install-dev

# Set up pre-commit linting hooks:
pre-commit install
```

## To view the docs in your browser:

Navigate to top-level balsam directory (where `mkdocs.yml` is located) and run:
```
mkdocs serve
```

Follow the link to the documentation. Docs are markdown files in the `balsam/docs` subdirectory and can be edited 
on-the-fly.  The changes will auto-refresh in the browser window.
