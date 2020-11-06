import click
from pathlib import Path
import shutil
from balsam.config import SiteConfig, ClientSettings, Settings
from .utils import load_site_config
from balsam.site.service import update_site_from_config


@click.group()
def site():
    """
    Setup or manage your Balsam sites
    """
    pass


def load_settings_comments(settings_dirs):
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
def init(site_path, hostname):
    """
    Create a new balsam site at SITE-PATH

    balsam site init path/to/site
    """
    import inquirer

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

    SiteConfig.new_site_setup(
        site_path=site_path, default_site_path=default_site_path, hostname=hostname
    )

    click.echo(f"New Balsam site set up at {site_path}")


@site.command()
@click.argument("src", type=click.Path(exists=True, file_okay=False))
@click.argument("dest", type=click.Path(exists=False, writable=True))
def mv(src, dest):
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
def rm(path):
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
def rename(path, name):
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
def ls():
    """
    List my balsam sites
    """
    client = ClientSettings.load_from_home().build_client()
    qs = client.Site.objects.all()
    for site in qs:
        click.echo(str(site))
        click.echo("---\n")


@site.command()
def sync():
    """
    Sync changes in local settings.yml with Balsam online
    """
    cf = load_site_config()
    client = cf.client
    site = client.Site.objects.get(site_id=cf.settings.site_id)
    update_site_from_config(site, cf.settings)


@site.command()
def sample_settings():
    path = Path("balsam-sample-settings.yml")
    if path.exists():
        click.echo(f"{path} already exists")
    else:
        Settings().save(path)
        click.echo(f"Wrote a sample settings file to {path}")
