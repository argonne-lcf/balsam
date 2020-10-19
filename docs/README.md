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
pip install -e .[dev,server]

pre-commit install
```

On commit, code is auto-formatted with `black` and linted with `flake8`.  Linting errors will cause commit to fail.

## Install Redis

Inside your virtualenv, run the script below to install Redis (or DIY):
```
bash redis-install.sh
```
This puts the Redis binary in your virtualenv bin

## Install Postgres

If `which pg_ctl`  does not show a Postgres on your system, get the postgres binaries from https://www.enterprisedb.com/download-postgresql-binaries .
You only need to unzip and add the postgres bin/ to your PATH.


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
pytest balsam/api/
```

Generate HTML report locally:
```bash
$ coverage html
$ open htmlcov/index.html
```
