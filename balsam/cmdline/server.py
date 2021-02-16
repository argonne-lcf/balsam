import json
import os
import signal
import subprocess
import tempfile
from pathlib import Path
from typing import Union

import click
import jinja2

import balsam.server

REDIS_TMPL = Path(balsam.server.__file__).parent.joinpath("redis.conf.tmpl")

# NOTE: lazy-import balsam.util.postgres inside each CLI handler
# Because psycopg2 is slow to import and would bog down the entire CLI
# if it were imported here!


@click.group()
def server() -> None:
    """
    Deploy and manage a local Balsam server
    """
    pass


@server.command()
@click.option("-p", "--path", required=True, type=click.Path(exists=True, file_okay=False))
def down(path: Union[str, Path]) -> None:
    """
    Shut down the Balsam server (Postgres, Redis, and Gunicorn)
    """
    path = Path(path).resolve()
    pid_files = ["balsamdb/postmaster.pid", "redis.pid", "gunicorn.pid"]
    services = ["PostgreSQL", "Redis", "Gunicorn"]
    for serv, pid_file in zip(services, pid_files):
        try:
            with open(path.joinpath(pid_file)) as fp:
                pid = int(fp.readline())
        except (FileNotFoundError, ValueError):
            click.echo(f"Could not read pidfile for stopping {serv}")
        else:
            click.echo(f"Sending SIGTERM to {serv} (pid={pid})")
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                click.echo(f"No process found with pid={pid}")


@server.command()
@click.option("-p", "--path", required=True, type=click.Path(exists=True, file_okay=False))
@click.option("-b", "--bind", default="0.0.0.0:8000")
@click.option("-l", "--log-level", default="debug")
@click.option("-w", "--num-workers", default=1, type=int)
def up(path: Union[Path, str], bind: str, log_level: str, num_workers: int) -> None:
    """
    Starts up the services comprising the Balsam server (Postgres, Redis, and Gunicorn)
    """
    from balsam.util import postgres as pg

    path = Path(path).resolve()

    click.echo("Starting Redis daemon")
    start_redis(path)
    db_path = path.joinpath("balsamdb").as_posix()
    pg.start_db(db_path)
    dsn = pg.load_dsn(db_path)
    pg.configure_balsam_server_from_dsn(dsn)
    start_gunicorn(path, bind, log_level, num_workers)


@server.command()
@click.option("-p", "--path", required=True, type=click.Path(exists=False, writable=True))
@click.option("-b", "--bind", default="0.0.0.0:8000")
@click.option("-l", "--log-level", default="debug")
@click.option("-w", "--num-workers", default=1, type=int)
def deploy(path: Union[Path, str], bind: str, log_level: str, num_workers: int) -> None:
    """
    Create a new Balsam database and API server instance
    """
    path = Path(path).resolve()
    if path.exists():
        raise click.BadParameter(f"{path} already exists")

    from balsam.util import postgres as pg

    click.echo("Creating database")
    pw_dict = pg.create_new_db(db_path=path.joinpath("balsamdb"))
    click.echo("Database created!")

    # Point balsam.server.settings at new DSN:
    dsn = pg.configure_balsam_server(**pw_dict)

    click.echo("Running alembic migrations")
    pg.run_alembic_migrations(dsn)
    click.echo("Migrations complete!")

    write_redis_conf(path.joinpath("redis.conf"))
    click.echo("Starting Redis daemon")
    start_redis(path)
    start_gunicorn(path, bind, log_level, num_workers)


def write_redis_conf(conf_path: Path) -> None:
    tmpl = jinja2.Template(REDIS_TMPL.read_text())
    with tempfile.NamedTemporaryFile(prefix="redis-balsam", suffix=".sock") as fp:
        tmp_name = Path(fp.name).resolve().as_posix()
    conf = tmpl.render({"unix_sock_path": tmp_name, "run_path": str(conf_path.parent)})
    with open(conf_path, "w") as f:
        f.write(conf)
    click.echo(f"Wrote redis conf to: {conf_path}")


def start_redis(path: Path, config_filename: str = "redis.conf") -> None:
    config_file = path.joinpath(config_filename)
    with open(config_file) as fp:
        for line in fp:
            if line.strip().startswith("unixsocket"):
                sock_file = line.split()[1]
                os.environ["balsam_redis_params"] = json.dumps({"unix_socket_path": sock_file})
    proc = subprocess.run(
        f"redis-server {config_file} --daemonize yes",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        encoding="utf-8",
        shell=True,
        check=False,
        cwd=path,
    )
    if proc.returncode != 0:
        click.echo(f"Error in starting Redis:\n{proc.stdout}")
        raise RuntimeError
    click.echo("Started redis daemon")


def start_gunicorn(path: Path, bind: str = "0.0.0.0:8000", log_level: str = "debug", num_workers: int = 1) -> None:
    args = [
        "gunicorn",
        "-k",
        "uvicorn.workers.UvicornWorker",
        "--bind",
        bind,
        "--log-level",
        log_level,
        "--access-logfile",
        "-",
        "--name",
        f"balsam-server[{path}]",
        "--workers",
        str(num_workers),
        "--pid",
        path.joinpath("gunicorn.pid").as_posix(),
        "balsam.server.main:app",
    ]
    with open(path.joinpath("gunicorn.out"), "w") as fp:
        p = subprocess.Popen(args, stdout=fp, stderr=subprocess.STDOUT, cwd=path)
    click.echo(f"Started gunicorn at {bind} (pid={p.pid})")


@server.group()
def db() -> None:
    """
    Setup or manage a local Postgres DB

    All subcommands require either:

        1) db-path as the first positional argument

        2) Environment variable BALSAM_DB_PATH
    """


@db.command()
@click.argument("db-path", type=click.Path(writable=True))
def init(db_path: Union[Path, str]) -> None:
    """
    Setup & start a new Postgres DB

    Sets up the Balsam database and runs DB migrations
    The DB connection info is written to server-info.yml
    """
    from balsam.util import postgres

    db_path = Path(db_path)
    if db_path.exists():
        raise click.BadParameter(f"The path {db_path} already exists")

    pw_dict = postgres.create_new_db(db_path, database="balsam")
    dsn = postgres.configure_balsam_server(**pw_dict)
    postgres.run_alembic_migrations(dsn)


@db.command()
def migrate() -> None:
    """
    Update DB schema (run after upgrading Balsam version)
    """
    import balsam.server
    from balsam.util import postgres

    dsn = balsam.server.settings.database_url
    click.echo(f"Running alembic migrations for {dsn}")
    postgres.run_alembic_migrations(dsn, downgrade=None)
    click.echo("Migrations complete!")


@db.command()
@click.argument("db-path", envvar="BALSAM_DB_PATH", type=click.Path(exists=True))
def start(db_path: Union[Path, str]) -> None:
    """
    Start a Postgres DB server locally, if not already running
    """
    from balsam.util import DirLock, postgres

    db_path = Path(db_path)

    # Avoid race condition when 2 scripts call balsam db start
    with DirLock(db_path, "db"):
        try:
            pw_dict = postgres.load_pwfile(db_path)
        except FileNotFoundError:
            raise click.BadParameter(f"There is no {postgres.SERVER_INFO_FILENAME} in {db_path}")

        try:
            postgres.connections_list(**pw_dict)
        except postgres.OperationalError:
            click.echo("Cannot reach DB, will restart...")
        else:
            click.echo("DB is already running!")
            return

        host, port = postgres.identify_hostport()
        pw_dict.update(host=host, port=port)
        postgres.write_pwfile(db_path, **pw_dict)
        postgres.mutate_conf_port(db_path, port)
        postgres.start_db(db_path)


@db.command()
@click.argument("db-path", envvar="BALSAM_DB_PATH", type=click.Path(exists=True))
def stop(db_path: Union[str, Path]) -> None:
    """
    Stop a local Postgres DB server process
    """
    from balsam.util import DirLock, postgres

    with DirLock(db_path, "db"):
        postgres.stop_db(db_path)


@db.command()
@click.argument("db-path", envvar="BALSAM_DB_PATH", type=click.Path(exists=True))
def connections(db_path: Union[Path, str]) -> None:
    """
    List currently open database connections
    """
    from balsam.util import postgres

    try:
        pw_dict = postgres.load_pwfile(db_path)
    except FileNotFoundError:
        raise click.BadParameter(f"There is no {postgres.SERVER_INFO_FILENAME} in {db_path}")

    connections = postgres.connections_list(**pw_dict)
    for conn in connections:
        click.echo(conn)


@db.command()
@click.argument("db-path", envvar="BALSAM_DB_PATH", type=click.Path(exists=True))
@click.argument("user")
def add_user(db_path: Union[Path, str], user: str) -> None:
    """
    Add a new authorized user to the DB

    Generates a {user}-server-info.yml for the user
    """
    from balsam.util import postgres

    try:
        pw_dict = postgres.load_pwfile(db_path)
    except FileNotFoundError:
        raise click.BadParameter(f"There is no {postgres.SERVER_INFO_FILENAME} in {db_path}")

    postgres.create_user_and_pwfile(new_user=user, **pw_dict)


@db.command()
@click.argument("db-path", envvar="BALSAM_DB_PATH", type=click.Path(exists=True))
@click.argument("user")
def drop_user(db_path: Union[str, Path], user: str) -> None:
    """
    Remove a user from the DB
    """
    from balsam.util import postgres

    try:
        pw_dict = postgres.load_pwfile(db_path)
    except FileNotFoundError:
        raise click.BadParameter(f"There is no {postgres.SERVER_INFO_FILENAME} in {db_path}")

    postgres.drop_user(deleting_user=user, **pw_dict)
    click.echo(f"Dropped user {user}")


@db.command()
@click.argument("db-path", envvar="BALSAM_DB_PATH", type=click.Path(exists=True))
def list_users(db_path: Union[str, Path]) -> None:
    """
    List authorized DB users
    """
    from balsam.util import postgres

    try:
        pw_dict = postgres.load_pwfile(db_path)
    except FileNotFoundError:
        raise click.BadParameter(f"There is no {postgres.SERVER_INFO_FILENAME} in {db_path}")

    user_list = postgres.list_users(**pw_dict)
    for user in user_list:
        click.echo(user)
