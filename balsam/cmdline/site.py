import click
from pathlib import Path
import shutil
from balsam.config import SiteConfig, ClientSettings, Settings


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
    import inquirer

    site_path = Path(site_path).absolute()
    default_dirs = {v.name: v for v in SiteConfig.load_default_config_dirs()}
    site_prompt = inquirer.List(
        "default_dir",
        message=f"Select a default configuration to initialize your Site {site_path.name}",
        choices=list(default_dirs.keys()),
        carousel=True,
    )

    if site_path.exists():
        raise click.BadParameter(f"{site_path} already exists")

    selected = inquirer.prompt([site_prompt])["default_dir"]
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
    client = ClientSettings.load_from_home().build_client()

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
    client = ClientSettings.load_from_home().build_client()
    site = client.Site.objects.get(id=cf.settings.site_id)

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
    cf = SiteConfig(path)
    client = ClientSettings.load_from_home().build_client()
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
def sample_settings():
    path = Path("balsam-sample-settings.yml")
    if path.exists():
        click.echo(f"{path} already exists")
    else:
        Settings().save(path)
        click.echo(f"Wrote a sample settings file to {path}")
