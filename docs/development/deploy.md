# Installing and running the API server

## Using Docker-Compose

The simplest way to run a test server instance is with 
[Docker Compose](https://docs.docker.com/compose/).
Run this command in the root `balsam/` directory:

```
docker-compose up -d
```

This will start the API server on `localhost:8000` along with Postgres and Redis.
You can alter the port and other server configuration by changing environment variables
in the `docker-compose.yml` file.

## Using Singularity

An analogous deployment for systems running Singularity is `singularity-deploy.sh`.  You may configure the server environment in this script and launch the API containers with:

```
source singularity-deploy.sh
```

## Using your host OS

A bare metal deployment requires PostgreSQL (Redis is optional). If these requiremenst are in place, you can create a new server instance with:
```
balsam server deploy -p /path/to/server-dir
```

This will create a new directory `server-dir` housing the database and log files.  You may start and stop the database with `balsam server up` and `balsam server down`, respectively.

### Install Postgres

If `which pg_ctl`  does not show a Postgres on your system, [get the Postgres binaries](https://www.enterprisedb.com/download-postgresql-binaries).
You only need to unzip and add the postgres bin/ to your PATH.

### Install Redis (Optional)

Inside your virtualenv, run the `redis-install.sh` script to install Redis (or DIY). This will copy the built Redis binary in your virtualenv `bin/`.