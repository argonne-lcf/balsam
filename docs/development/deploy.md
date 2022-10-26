# Balsam Service Deployment

## Installation

### Docker Compose: quick setup (recommended)
Docker Compose can be used to manage the PostgreSQL,  Redis, and Balsam server containers with a single command:

```console
$ cd balsam/
$ git pull
$ cp .env.example .env
$ vim .env  # Configure here
$ vim balsam/server/gunicorn.conf.example.py # And here
$ docker-compose up --build -d
```

### Manual Installation
Balsam can be installed into a Python environment in two ways. 
User-mode installation with `pip install --pre balsam` or `pip install -e .` fetches
end-user package dependencies with flexible version ranges.  **This will not suffice for running a Balsam server.**
To install the server, you must use the second option:

```console
pip install -r requirements/deploy.txt
```

This installs all the necessary dependencies with pinned versions for a reproducible deployment environment.

Next, you will need to run a PostgreSQL database dedicated to Balsam.  If `which pg_ctl`  does not show a Postgres on your system, [get the Postgres binaries](https://www.enterprisedb.com/download-postgresql-binaries).
You only need to unzip and add the postgres bin/ to your PATH.
Follow the PostgreSQL docuemntation to create a new database cluster with `initdb`, configure and start up the database server, and create an empty database with `createdb`.
The canonical name for the database is `balsam` but any name can be used. The DSN for Balsam to connect to the database is configured via `BALSAM_DATABASE_URL`. Refer to the
`.env.example` file and [Pydantic documentation for URL parsing](https://pydantic-docs.helpmanual.io/usage/types/#urls).

Running Redis is optional and needed only for the Balsam event streaming WebSocket functionality.  Inside your virtualenv, run the `redis-install.sh` script to install Redis (or DIY). This will copy the built Redis binary in your virtualenv `bin/`. Run the Redis server and configure the Balsam-Redis connection via the `BALSAM_REDIS_PARAMS` environment variable, which should be a JSON-formatted string as shown in the `.env.example` file.

## Balsam Server Configuration

Regardless of installation method, the server can be configured in a few places:

1. Environment variables are read from the `.env` file in the project root directory. Refer to, copy, and modify the `.env.example` example file.
2. Gunicorn-specific configuration is contained in the file referenced by `GUNICORN_CONFIG_FILE`.  Refer to the example in `balsam/server/gunicorn.conf.example.py`
3. Any additional settings listed in `balsam/server/conf.py` can also be controlled through the `.env` file or environment variables. The environment variables should be formed by combining the appropriate `env_prefix` with the setting name.

!!! note "Keeping Balsam and Gunicorn Config Separate"
    Settings internal to the Balsam web server application are defined with [Pydantic](https://pydantic-docs.helpmanual.io/usage/settings/)
    in `balsam/server/conf.py`.  This includes concerns such as where to find the database, how to perform logging, and how to perform user Authentication.

    This should not be conflated with outer-level server *environment* concerns that Balsam itself does not need to know.  Examples include which port the server is listening on, or how many copies of the underlying `uvicorn` web worker are running.  These ultimately depend on the deployment method. In the case of Docker Compose with Gunicorn, we break the config into the separate `gunicorn.conf.example.py` file and load it from within the Dockerfile's entrypoint.

## Database Migrations

Initially, the PostgreSQL database will be empty and have no tables defined.  To apply the latest Balsam database schema, you need to run the Alembic migrations:

```bash
# Make sure Postgres is up and running
# Make sure .env has the correct BALSAM_DATABASE_URL
$ balsam server migrate

# Or when using Docker:
$ docker-compose exec gunicorn balsam server migrate
```

## Stopping and Starting the Server

With Docker Compose, the server and its companion services are started/stopped with the  `docker-compose` subcommands `up` and `down`:

```bash
# Start, detached:
$ docker-compose up -d

# Stop:
$ docker-compose down
```

When running a bare-metal installation, you are responsible for having PostgreSQL (and optionally Redis) started up separately. Then, you may launch the Balsam web application with `gunicorn`:

```bash
$ gunicorn -c ./balsam/server/gunicorn.conf.example.py balsam.server.main:app
```

As a quick sanity check that the server is running and reachable, you can try to fetch the FastAPI docs:

```bash
$ curl localhost:8000/docs
```

## Updating the Server Code
Run `git pull` to update the server Python code. Because the source directory is mounted in the container, this can even be used to live-update the server when running with Docker Compose:

```console
$ git pull
$ docker kill --signal=SIGHUP gunicorn
```

The same applies when running without Docker.

!!! note "Live Update Limitations"
    Restarting gunicorn workers with `SIGHUP` avoids server downtime, but it will not apply changes to the container environment (i.e. any changes made to `docker-compose.yml` or `.env` will not propagate to the workers).  This method is only useful for updates to the gunicorn config or Python source code.

More generally, when there are changes to the container, Python environment, or configuration, you will want to rebuild the container and restart the service, which entails downtime:

```console
$ cd balsam/
$ docker-compose down
$ docker-compose build gunicorn
$ docker-compose up -d
```

## Database Backups

The script `balsam/deploy/db_snapshot.py` can be used with the accompanying
`service.example` file to set up a recurring service for dumping the Postgres
database to a file.  Copy the Python script to an appropriate location for the service,
modify the `service.example` file accordingly, and follow the instructions at the top of the
`service.example` file. 

The script uses the [basic postgres dump
functionality](https://www.postgresql.org/docs/9.1/backup-dump.html).  Backups
are performed locally, and should be replicated to a remote system.  The easiest
way within the CELS GCE environment is to set up a cron job to **pull** the database
backups.

```
15 * * * *  rsync -avz balsam-dev-01.alcf.anl.gov:/home/msalim/db-backups /nfs/gce/projects/balsam/backups/
```