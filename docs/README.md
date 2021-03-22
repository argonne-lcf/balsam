# User installation

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

# Developer/server-side installation

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
