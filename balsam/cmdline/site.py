import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, Union

import click
import psutil  # type: ignore

from balsam.config import ClientSettings, InvalidSettings, Settings, SiteConfig
from balsam.site.service import update_site_from_config

from .utils import load_site_config

PID_FILENAME = "balsam-service.pid"


@click.group()
def site() -> None:
    """
    Setup or manage your Balsam sites
    """
    pass


@site.command()
def start() -> None:
    """Start up the site daemon"""
    cf = load_site_config()
    site_dir = cf.site_path
    if site_dir.joinpath(PID_FILENAME).is_file():
        raise click.BadArgumentUsage(
            f"{PID_FILENAME} already exists in {site_dir}: "
            "This means the site is already running; to restart it, "
            "first use `balsam site stop`."
        )
    os.environ["BALSAM_SITE_PATH"] = site_dir.as_posix()
    p = subprocess.Popen(
        [sys.executable, "-m", "balsam.site.service.main"],
        cwd=site_dir,
    )
    time.sleep(0.2)
    if p.poll() is None:
        click.echo(f"Started Balsam site daemon [pid {p.pid}]")


@site.command()
def stop() -> None:
    """Stop site daemon"""
    cf = load_site_config()
    site_dir: Path = cf.site_path
    pid_file: Path = site_dir.joinpath(PID_FILENAME)
    if not pid_file.is_file():
        raise click.BadArgumentUsage(f"There is no {PID_FILENAME} in {site_dir}")

    try:
        service_pid: Union[str, int]
        service_host, service_pid = pid_file.read_text().split("\n")[:2]
        service_pid = int(service_pid)
    except ValueError:
        raise click.BadArgumentUsage(
            f"Failed to read {pid_file}: please delete it and manually kill the site daemon process!"
        )
    cur_host = socket.gethostname()
    if cur_host != service_host:
        raise click.BadArgumentUsage(
            f"The site daemon is running on {service_host}; cannot stop from current host: {cur_host}"
        )
    if not psutil.pid_exists(service_pid):
        raise click.BadArgumentUsage(
            f"Could not find process with PID {service_pid}. "
            f"Make sure the Balsam site daemon isn't running and delete {pid_file}"
        )
    try:
        service_proc = psutil.Process(pid=service_pid)
        service_proc.terminate()
    except (ProcessLookupError, psutil.ProcessLookupError):
        raise click.BadArgumentUsage(
            f"Could not find process with PID {service_pid}. "
            f"Make sure the Balsam site daemon isn't running and delete {pid_file}"
        )
    click.echo(f"Sent SIGTERM to Balsam site daemon [pid {service_pid}]")
    click.echo("Waiting for site daemon to shutdown...")
    with click.progressbar(range(12)) as bar:
        for i in bar:
            try:
                service_proc.wait(timeout=1)
            except psutil.TimeoutExpired:
                if i == 11:
                    raise click.BadArgumentUsage(
                        f"Site daemon did not shut down gracefully on its own; please kill it manually "
                        f"and delete {pid_file}"
                    )
            else:
                click.echo("\nSite daemon shutdown OK")
                break


def load_settings_comments(settings_dirs: Dict[str, Path]) -> Dict[str, str]:
    descriptions = {name: "" for name in settings_dirs}
    for name, dir in settings_dirs.items():
        firstline = dir.joinpath("settings.yml").read_text().split("\n")[0]
        firstline = firstline.strip()
        if firstline.startswith("#"):
            descriptions[name] = f'({firstline.lstrip("#").strip()})'
    return descriptions


@site.command()
@click.argument("site-path", type=click.Path(writable=True))
@click.option("-h", "--hostname")
def init(site_path: Union[str, Path], hostname: str) -> None:
    """
    Create a new balsam site at SITE-PATH

    balsam site init path/to/site
    """
    import inquirer  # type: ignore

    site_path = Path(site_path).absolute()
    default_dirs = {v.name: v for v in SiteConfig.load_default_config_dirs()}
    descriptions = load_settings_comments(default_dirs)
    choices = [f"{name}  {description}" for name, description in descriptions.items()]

    site_prompt = inquirer.List(
        "default_dir",
        message=f"Select a default configuration to initialize your Site {site_path.name}",
        choices=choices,
        carousel=True,
    )

    if site_path.exists():
        raise click.BadParameter(f"{site_path} already exists")

    selected = inquirer.prompt([site_prompt])["default_dir"]
    selected = selected.split()[0]
    default_site_path = default_dirs[selected]

    try:
        SiteConfig.new_site_setup(site_path=site_path, default_site_path=default_site_path, hostname=hostname)
    except (InvalidSettings, FileNotFoundError) as exc:
        click.echo(str(exc))
        sys.exit(1)

    click.echo(f"New Balsam site set up at {site_path}")


@site.command()
@click.argument("src", type=click.Path(exists=True, file_okay=False))
@click.argument("dest", type=click.Path(exists=False, writable=True))
def mv(src: Union[Path, str], dest: Union[Path, str]) -> None:
    """
    Move a balsam site

    balsam site mv /path/to/src /path/to/destination
    """
    cf = SiteConfig(src)

    src = Path(src).resolve()
    dest = Path(dest).resolve()

    if dest.exists():
        raise click.BadParameter(f"{dest} exists")

    shutil.move(src.as_posix(), dest)
    client = cf.client

    site = client.Site.objects.get(id=cf.settings.site_id)
    site.path = dest
    site.save()
    click.echo(f"Moved site to new path {dest}")


@site.command()
@click.argument("path", type=click.Path(exists=True, file_okay=False))
def rm(path: Union[str, Path]) -> None:
    """
    Remove a balsam site

    balsam site rm /path/to/site
    """
    cf = SiteConfig(path)
    client = cf.client
    site = client.Site.objects.get(id=cf.settings.site_id)
    jobcount = client.Job.objects.filter(site_id=site.id).count()
    warning = f"This will wipe out {jobcount} jobs inside!" if jobcount else ""

    if click.confirm(f"Do you really want to destroy {Path(path).name}? {warning}"):
        site.delete()
        shutil.rmtree(path)
        click.echo(f"Deleted site {path}")


@site.command()
@click.argument("path", type=click.Path(exists=True, file_okay=False))
@click.argument("name")
def rename(path: Union[str, None], name: str) -> None:
    """
    Change the hostname of a balsam site
    """
    cf = SiteConfig(path)
    client = cf.client
    site = client.Site.objects.get(id=cf.settings.site_id)
    site.hostname = name
    site.save()
    click.echo("Renamed site {site.id} to {site.hostname}")


@site.command()
def ls() -> None:
    """
    List my balsam sites
    """
    client = ClientSettings.load_from_home().build_client()
    qs = client.Site.objects.all()
    for site in qs:
        click.echo(str(site))
        click.echo("---\n")


@site.command()
def sync() -> None:
    """
    Sync changes in local settings.yml with Balsam online
    """
    cf = load_site_config()
    client = cf.client
    site = client.Site.objects.get(id=cf.settings.site_id)
    update_site_from_config(site, cf.settings)


@site.command()
def sample_settings() -> None:
    """
    Print a sample settings.yml site configuration
    """
    click.echo(Settings().dump_yaml())
