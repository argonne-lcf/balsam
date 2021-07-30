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
# Because psycopg2 is slow to import


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
@click.option("--log/--no-log", is_flag=True, default=False)
def up(path: Union[Path, str], log: bool) -> None:
    """
    Starts up the services comprising the Balsam server (Postgres, Redis, and Gunicorn)
    """
    from balsam.util import postgres as pg

    path = Path(path).resolve()
    gunicorn_config_file = Path(balsam.server.__file__).parent / "gunicorn.conf.example.py"
    assert gunicorn_config_file.is_file()

    click.echo("Starting Redis daemon")
    try:
        start_redis(path)
    except RuntimeError:
        click.echo("Skipping Redis")
    db_path = path.joinpath("balsamdb").as_posix()
    pg.start_db(db_path)
    dsn = pg.load_dsn(db_path)
    pg.configure_balsam_server_from_dsn(dsn)

    os.environ["BALSAM_LOG_DIR"] = path.as_posix() if log else ""
    args = [
        "gunicorn",
        "-c",
        gunicorn_config_file.as_posix(),
        "balsam.server.main:app",
    ]
    p = subprocess.Popen(args, cwd=path)
    click.echo(f"Started gunicorn (pid={p.pid})")


@server.command()
@click.option("-p", "--path", required=True, type=click.Path(exists=False, writable=True))
def deploy(path: Union[Path, str]) -> None:
    """
    Create a new Balsam database and API server instance
    """
    path = Path(path).resolve()
    if path.exists():
        raise click.BadParameter(f"{path} already exists")

    from balsam.util import postgres as pg

    gunicorn_config_file = Path(balsam.server.__file__).parent / "gunicorn.conf.example.py"
    assert gunicorn_config_file.is_file()

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
    try:
        start_redis(path)
    except RuntimeError:
        click.echo("Skipping Redis")

    os.environ["BALSAM_LOG_DIR"] = path.as_posix()
    args = [
        "gunicorn",
        "-c",
        gunicorn_config_file.as_posix(),
        "balsam.server.main:app",
    ]
    p = subprocess.Popen(args, cwd=path)
    click.echo(f"Started gunicorn (pid={p.pid})")


@server.command()
def migrate() -> None:
    from balsam.util import postgres as pg

    dsn = balsam.server.Settings().database_url
    click.echo("Running alembic migrations")
    pg.run_alembic_migrations(dsn)
    click.echo("Migrations complete!")


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
