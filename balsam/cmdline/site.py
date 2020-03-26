import click
from pathlib import Path
import shutil
from balsam.config import BalsamComponentFactory, ClientSettings
from balsam.api import Site


@click.group()
def site():
    """
    Setup or manage your Balsam sites
    """
    pass


@site.command()
@click.argument("site-path", type=click.Path(writable=True))
@click.option("-h", "--hostname")
def init(site_path, hostname):
    """
    Create a new balsam site at SITE-PATH

    balsam site init path/to/site
    """
    site_path = Path(site_path).absolute()
    if site_path.exists():
        raise click.BadParameter(f"{site_path} already exists")

    BalsamComponentFactory.new_site_setup(site_path=site_path, hostname=hostname)

    click.echo(f"New Balsam site set up at {site_path}")


@site.command()
@click.argument("src", type=click.Path(exists=True, file_okay=False))
@click.argument("dest", type=click.Path(exists=False, writable=True))
def mv(src, dest):
    """
    Move a balsam site

    balsam site mv /path/to/src /path/to/destination
    """
    cf = BalsamComponentFactory(src)

    if Path(dest).exists():
        raise click.BadParameter(f"{dest} exists")

    shutil.move(src, dest)
    ClientSettings.load_from_home().build_client()

    site = Site.objects.get(pk=cf.settings.site_id)
    site.path = dest
    site.save()
    click.echo(f"Moved site to new path {dest}")


@site.command()
@click.argument("path", type=click.Path(exists=True, file_okay=False))
def rm(path):
    """
    Remove a balsam site

    balsam site rm /path/to/site
    """
    cf = BalsamComponentFactory(path)
    ClientSettings.load_from_home().build_client()
    site = Site.objects.get(pk=cf.settings.site_id)

    if click.confirm(f"Do you really want to destroy {path}?"):
        site.delete()
        shutil.rmtree(path)
        click.echo(f"Deleted site {path}")


@site.command()
@click.argument("path", type=click.Path(exists=True, file_okay=False))
@click.argument("name")
def rename(path, name):
    """
    Change the hostname of a balsam site
    """
    cf = BalsamComponentFactory(path)
    ClientSettings.load_from_home().build_client()
    site = Site.objects.get(pk=cf.settings.site_id)
    site.hostname = name
    site.save()
    click.echo("Renamed site {site.pk} to {site.hostname}")


@site.command()
def ls():
    """
    List my balsam sites
    """
    ClientSettings.load_from_home().build_client()
    qs = Site.objects.all()
    for site in qs:
        click.echo(f"{site.pk} {site.hostname} {site.path}")
