import socket
from pathlib import Path

import click
import yaml

from balsam._api import ApplicationDefinition
from balsam.config import ClientSettings
from balsam.site.app import sync_apps

from .utils import check_killable, filter_by_sites, kill_site, load_site_config, start_site


@click.group()
def app() -> None:
    """
    Sync and manage Balsam applications
    """
    pass


@app.command()
def sync() -> None:
    """
    Sync local ApplicationDefinitions with Balsam
    """
    cf = load_site_config()
    sync_apps(cf)
    kill_pid = check_killable(cf)
    if kill_pid is not None:
        click.echo(f"Restarting Site {cf.site_path}")
        kill_site(cf, kill_pid)
        proc = start_site(cf.site_path)
        click.echo(f"Restarted Balsam site daemon [pid {proc.pid}] on {socket.gethostname()}")


@app.command()
@click.option("-v", "--verbose", is_flag=True)
@click.option("-s", "--site", "site_selector", default="")
def ls(site_selector: str, verbose: bool) -> None:
    """
    List Apps

    1) View apps across all sites

        balsam app ls --site=all

    2) Filter apps by specific site IDs or Path fragments

        balsam app ls --site=123,my_site_folder
    """
    client = ClientSettings.load_from_file().build_client()
    qs = client.App.objects.all()
    qs = filter_by_sites(qs, site_selector)
    if verbose:
        reprs = [yaml.dump(app.display_dict(), sort_keys=False, indent=4) for app in qs]
        print(*reprs, sep="\n----\n")
    else:
        sites = {site.id: site for site in client.Site.objects.all()}
        click.echo(f"{'ID':>5s}   {'ClassPath':>18s}   {'Site':<20s}")
        apps = sorted(list(qs), key=lambda app: app.site_id)
        for a in apps:
            site = sites[a.site_id]
            site_str = f"{site.name}"
            click.echo(f"{a.id:>5d}   {a.class_path:>18s}   {site_str:<20s}")
