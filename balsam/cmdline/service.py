from pathlib import Path
import os
import psutil
import sys
import time
import subprocess
import socket
import click
from .utils import load_site_config
from balsam.site.service import main

service_main = Path(main.__file__)
PID_FILENAME = "balsam-service.pid"


@click.group()
@click.pass_context
def service(ctx):
    """Start/stop a Site's Balsam service"""
    ctx.obj = load_site_config()


@service.command()
@click.pass_context
def start(ctx):
    """Start service"""
    site_dir = ctx.obj.site_path
    if site_dir.joinpath(PID_FILENAME).is_file():
        raise click.BadArgumentUsage(
            f"{PID_FILENAME} already exists in {site_dir}: "
            "This means the service is already running; to restart it, "
            "first use `balsam service stop`."
        )
    os.environ["BALSAM_SITE_PATH"] = site_dir.as_posix()
    # outfile = ctx.obj.log_path.joinpath("service.out")
    # with open(outfile, "wb") as fp:
    p = subprocess.Popen(
        [sys.executable, "-m", "balsam.site.service.main"],
        cwd=site_dir,
        # stdout=fp,
        # stderr=subprocess.STDOUT,
    )
    time.sleep(0.2)
    if p.poll() is None:
        click.echo(f"Started Balsam service [pid {p.pid}]")
        click.echo(f"args: {p.args}")


@service.command()
@click.pass_context
def stop(ctx):
    """Stop service"""
    site_dir: Path = ctx.obj.site_path
    pid_file: Path = site_dir.joinpath(PID_FILENAME)
    if not pid_file.is_file():
        raise click.BadArgumentUsage(f"There is no {PID_FILENAME} in {site_dir}")

    try:
        service_host, service_pid = pid_file.read_text().split("\n")[:2]
        service_pid = int(service_pid)
    except ValueError:
        raise click.BadArgumentUsage(
            f"Failed to read {pid_file}: please delete it and manually kill the service process!"
        )
    cur_host = socket.gethostname()
    if cur_host != service_host:
        raise click.BadArgumentUsage(
            f"The service is running on {service_host}; cannot stop from current host: {cur_host}"
        )
    if not psutil.pid_exists(service_pid):
        raise click.BadArgumentUsage(
            f"Could not find process with PID {service_pid}. "
            f"Make sure the Balsam service isn't running and delete {pid_file}"
        )
    try:
        service_proc = psutil.Process(pid=service_pid)
        service_proc.terminate()
    except (ProcessLookupError, psutil.ProcessLookupError):
        raise click.BadArgumentUsage(
            f"Could not find process with PID {service_pid}. "
            f"Make sure the Balsam service isn't running and delete {pid_file}"
        )
    click.echo(f"Sent SIGTERM to Balsam service [pid {service_pid}]")
    click.echo(f"Waiting for service to shutdown...")
    with click.progressbar(range(12)) as bar:
        for i in bar:
            try:
                service_proc.wait(timeout=1)
            except psutil.TimeoutExpired:
                if i == 11:
                    raise click.BadArgumentUsage(
                        f"Service did not shut down gracefully on its own; please kill it manually "
                        f"and delete {pid_file}"
                    )
            else:
                click.echo("\nService shutdown OK")
                break
