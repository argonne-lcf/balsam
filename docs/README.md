# User installation

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

# Developer/deployment installation
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


## Install Redis

Inside your virtualenv, run the script below to install Redis (or DIY):
```
bash redis-install.sh
```
This puts the Redis binary in your virtualenv bin

## Install Postgres

If `which pg_ctl`  does not show a Postgres on your system, [get the Postgres binaries](https://www.enterprisedb.com/download-postgresql-binaries).
You only need to unzip and add the postgres bin/ to your PATH.

### Deploying Balsam Server locally (bare-metal)

Use the `balsam server deploy` command line interface to automate Postgres, Redis, and Gunicorn Management on bare metal.
