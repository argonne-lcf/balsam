import click
import yaml

from balsam.config import ClientSettings

from .utils import filter_by_sites


@click.group()
def app() -> None:
    """
    Manage Balsam applications
    """
    pass


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
        click.echo(f"{'ID':>5s}   {'Name':>18s}   {'Site':<20s}")
        apps = sorted(list(qs), key=lambda app: app.site_id)
        for a in apps:
            site = sites[a.site_id]
            click.echo(f"{a.id:>5d}   {a.name:>18s}   {site.name:<20s}")
