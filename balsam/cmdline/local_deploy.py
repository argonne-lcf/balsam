import os
import jinja2
from pathlib import Path
import signal
import tempfile
import subprocess
import click
import balsam.server
import balsam.server.models.alembic as alembic

MIGRATION_DIR = Path(alembic.__file__).parent
REDIS_TMPL = Path(balsam.server.__file__).parent.joinpath("redis.conf.tmpl")


@click.group()
def server():
    """
    Deploy and manage a local Balsam server
    """
    pass


@server.command()
@click.option(
    "-p", "--path", required=True, type=click.Path(exists=True, file_okay=False)
)
def down(path):
    """
    Shut down the Balsam server (Postgres, Redis, and Gunicorn)
    """
    path = Path(path).resolve()
    pid_files = ["balsamdb/postmaster.pid", "redis.pid", "gunicorn.pid"]
    services = ["PostgreSQL", "Redis", "Gunicorn"]
    for serv, pid_file in zip(services, pid_files):
        try:
            with open(path.joinpath(pid_file)) as fp:
                pid = fp.readline()
            pid = int(pid)
        except (FileNotFoundError, ValueError):
            click.echo(f"Could not read pidfile for stopping {serv}")
        else:
            click.echo(f"Sending SIGTERM to {serv} (pid={pid})")
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                click.echo(f"No process found with pid={pid}")


@server.command()
@click.option(
    "-p", "--path", required=True, type=click.Path(exists=True, file_okay=False)
)
@click.option("-b", "--bind", default="0.0.0.0:8000")
@click.option("-l", "--log-level", default="debug")
@click.option("-w", "--num-workers", default=1, type=int)
def up(path, bind, log_level, num_workers):
    """
    Starts up the services comprising the Balsam server (Postgres, Redis, and Gunicorn)
    """
    path = Path(path).resolve()
    click.echo("Starting Redis daemon")
    start_redis(path)
    start_gunicorn(path, bind, log_level, num_workers)
    from balsam.util import postgres as pg

    pg.start_db(path.joinpath("balsamdb").as_posix())


@server.command()
@click.option(
    "-p", "--path", required=True, type=click.Path(exists=False, writable=True)
)
@click.option("-b", "--bind", default="0.0.0.0:8000")
@click.option("-l", "--log-level", default="debug")
@click.option("-w", "--num-workers", default=1, type=int)
def deploy(path, bind, log_level, num_workers):
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
    user = pw_dict["username"]
    passwd = pw_dict["password"]
    host = pw_dict["host"]
    port = pw_dict["port"]
    database = pw_dict["database"]
    dsn = f"postgresql://{user}:{passwd}@{host}:{port}/{database}"

    balsam.server.settings.database_url = dsn

    click.echo("Running alembic migrations")
    pg.run_alembic_migrations(MIGRATION_DIR, dsn)
    click.echo("Migrations complete!")

    write_redis_conf(path.joinpath("redis.conf"))
    click.echo("Starting Redis daemon")
    start_redis(path)
    start_gunicorn(path, bind, log_level, num_workers)


def write_redis_conf(conf_path):
    tmpl = jinja2.Template(REDIS_TMPL.read_text())
    with tempfile.NamedTemporaryFile(prefix="redis-balsam", suffix=".sock") as fp:
        tmp_name = Path(fp.name).resolve().as_posix()
    conf = tmpl.render({"unix_sock_path": tmp_name, "run_path": str(conf_path.parent)})
    with open(conf_path, "w") as fp:
        fp.write(conf)
    click.echo(f"Wrote redis conf to: {conf_path}")


def start_redis(path, config_filename="redis.conf"):
    config_file = path.joinpath(config_filename)
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
    pid_idx = proc.stdout.find("pid=")
    pid = proc.stdout[pid_idx + 4 :]
    pid = int(pid[: pid.find(",")])
    click.echo(f"Started redis daemon pid={pid}")
    # Redis conf takes care of pidfile


def start_gunicorn(path, bind="0.0.0.0:8000", log_level="debug", num_workers=1):
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
    click.echo(f"Started gunicorn (pid={p.pid})")
