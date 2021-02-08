[![codecov](https://codecov.io/gh/balsam-alcf/balsam/branch/master/graph/badge.svg)](https://codecov.io/gh/balsam-alcf/balsam)

[![Build Status](https://travis-ci.com/balsam-alcf/balsam.svg?branch=develop)](https://travis-ci.com/balsam-alcf/balsam)

# Setting up the repository

```
git clone https://github.com/balsam-alcf/balsam.git
cd balsam
git checkout fastapi

python --version  # Python 3.6+
python -m venv env # Create env
source env/bin/activate

# Installs balsam with optional dev, server, and docs dependencies
# (A typical user would omit the square-brackets when using a hosted Balsam)
pip install -e .[dev,server,docs]

pre-commit install
```
Pre-commit installs hooks in the `.git/hooks` directory.
On commit, code is auto-formatted with `black` and linted with `flake8`.  Linting errors will cause commit to fail.

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

If `which pg_ctl`  does not show a Postgres on your system, get the postgres binaries from https://www.enterprisedb.com/download-postgresql-binaries .
You only need to unzip and add the postgres bin/ to your PATH.

### Deploying Balsam Server locally

Use the `balsam server deploy` command line interface to automate Postgres, Redis, and Gunicorn Management.

## Provision the database

```
initdb -U postgres dev-db
pg_ctl -D dev-db -l dev-db/postgres.log start
createdb -U postgres balsam
createdb -U postgres balsam-test

cd balsam/server/models
alembic -x db=prod upgrade head
alembic -x db=test upgrade head
```

## Start up Redis
```
cd balsam/server
redis-server default-redis.conf --daemonize yes
```


## Testing
From the top `balsam` directory, test the client API with PyTest:
Test the DRF backend with PyTest:

```bash
pytest tests/api
```
This will run an extensive set of tests with the test database (balsam-test) as a backend. It will automatically start and stop Gunicorn as needed.

You should see all tests pass successfully.

## Hosting the API yourself

The API server is configurable at balsam/server/conf.py.  The defaults should suffice.  Run the server with Gunicorn:

```
gunicorn -k uvicorn.workers.UvicornWorker --bind "localhost:8080" --log-level debug balsam.server.main:app
```

## Interacting with the API Python Client

Run the example at `balsam/examples/client_api.py` or try it out interactively.