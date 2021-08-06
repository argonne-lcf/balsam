import shutil
import socket
import sys
from pathlib import Path
from typing import Optional, Union

import click

from balsam.config import ClientSettings, InvalidSettings, Settings, SiteConfig, site_builder
from balsam.site.app import sync_apps
from balsam.util import globus_auth

from .utils import (
    PID_FILENAME,
    check_killable,
    get_pidfile,
    is_site_active,
    kill_site,
    load_site_config,
    read_pidfile,
    start_site,
)


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
    if not get_pidfile(cf).is_file():
        click.echo(f"Starting Balsam site at {cf.site_path}")
        proc = start_site(cf.site_path)
        click.echo(f"Started Balsam site daemon [pid {proc.pid}] on {socket.gethostname()}")
        return None

    host: Optional[str]
    pid: Optional[int]
    try:
        host, pid = read_pidfile(cf)
    except Exception:
        raise click.BadArgumentUsage(
            f"{PID_FILENAME} already exists in {cf.site_path}: "
            "This means the site is already running; to restart it, "
            "first use `balsam site stop`."
        )
    else:
        raise click.BadArgumentUsage(
            f"The site is already running on {host} [PID {pid}].\nTo restart it, first use `balsam site stop`."
        )


@site.command()
def stop() -> None:
    """Stop site daemon"""
    cf = load_site_config()
    kill_pid = check_killable(cf, raise_exc=True)
    if kill_pid is not None:
        kill_site(cf, kill_pid)


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
    if site_path.exists():
        raise click.BadParameter(f"{site_path} already exists")

    path_conf_map = site_builder.load_default_configs()
    site_prompt = inquirer.List(
        "path",
        message="Select system",
        choices=[(conf.title, path) for path, conf in path_conf_map.items()],
        carousel=True,
    )
    selected_path = inquirer.prompt([site_prompt])["path"]

    try:
        cf = site_builder.new_site_setup(
            site_path=site_path,
            default_site_path=selected_path,
            default_site_conf=path_conf_map[selected_path],
            hostname=hostname,
        )
    except (InvalidSettings, FileNotFoundError) as exc:
        click.echo(str(exc))
        sys.exit(1)
    click.echo(f"New Balsam site set up at {site_path}")
    sync_apps(cf)


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

    shutil.move(src.as_posix(), dest.as_posix())
    client = cf.client

    site = client.Site.objects.get(id=cf.site_id)
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
    site = client.Site.objects.get(id=cf.site_id)
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
    site = client.Site.objects.get(id=cf.site_id)
    site.hostname = name
    site.save()
    click.echo("Renamed site {site.id} to {site.hostname}")


@site.command()
@click.option("-v", "--verbose", is_flag=True)
def ls(verbose: bool) -> None:
    """
    List my balsam sites
    """
    client = ClientSettings.load_from_file().build_client()
    qs = client.Site.objects.all()
    if verbose:
        for site in qs:
            click.echo(str(site))
            click.echo("---\n")
    else:
        click.echo(f"{'ID':>5s}   {'Hostname':>14s}   {'Path':<40s}   {'Active':<4s}")
        for s in qs:
            assert s.path is not None
            assert s.last_refresh is not None
            pathstr = s.path.as_posix()
            if len(pathstr) > 37:
                pathstr = "..." + pathstr[-37:]
            active = "Yes" if is_site_active(s) else "No"
            click.echo(f"{s.id:>5d}   {s.hostname:>14}   {pathstr:<40}   {active:<4}")


@site.command()
def sync() -> None:
    """
    Sync changes in local settings.yml with Balsam online
    """
    cf = load_site_config()
    cf.update_site_from_config()
    click.echo("Updated site.")
    kill_pid = check_killable(cf)
    if kill_pid is not None:
        click.echo(f"Restarting Site {cf.site_path}")
        kill_site(cf, kill_pid)
        proc = start_site(cf.site_path)
        click.echo(f"Restarted Balsam site daemon [pid {proc.pid}] on {socket.gethostname()}")


@site.command()
def sample_settings() -> None:
    """
    Print a sample settings.yml site configuration
    """
    click.echo(Settings().dump_yaml())


@site.command()
def globus_login() -> None:
    """
    Get credentials for the Globus CLI

    Necessary before any Globus CLI commands which require authentication will work
    This command directs you to the page necessary to permit the Globus CLI to make API
    calls for you, and gets the OAuth2 tokens needed to use those permissions.
    """
    # if not forcing, stop if user already logged in
    if globus_auth.check_logged_in():  # type: ignore
        click.echo("You are already logged in!")
        return

    globus_auth.do_link_auth_flow(force_new_client=True)  # type: ignore

    click.echo("You have successfully logged in to the Globus CLI!")
