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


@app.command()
@click.option("-n", "--name", "name_selector", default=None)
@click.option("-s", "--site", "site_selector", default="")
@click.option("-a", "--all", is_flag=True, default=False)
def rm(site_selector: str, name_selector: str, all: bool) -> None:
    """
    Remove Apps

    1) Remove named app

        balsam app rm -n hello_world

    1) Remove all apps across a site

        balsam app rm --all --site=123,my_site_folder

    2) Filter apps by specific site IDs or Path fragments

        balsam app rm -n hello_world --site=123,my_site_folder

    """
    client = ClientSettings.load_from_file().build_client()
    qs = client.App.objects.all()
    qs = filter_by_sites(qs, site_selector)

    if all and name_selector is not None:
        raise click.BadParameter("Specify app name or --all, but not both")
    elif not all and name_selector is None:
        raise click.BadParameter("Specify app name with -n or specify --all")
    else:
        app_list = []

        if all and site_selector == "":
            raise click.BadParameter("balsam app rm --all requires that you specify --site to remove jobs")
        elif all and site_selector != "":
            click.echo("THIS WILL DELETE ALL APPS IN SITE! CAUTION!")
            app_list = [a.name for a in list(qs)]
            num_apps = 0
            num_jobs = 0
        elif name_selector is not None:
            app_list = [name_selector]

        if len(app_list) > 0:
            for name in app_list:
                resolved_app = qs.get(name=name)
                resolved_id = resolved_app.id
                job_count = client.Job.objects.filter(app_id=resolved_id).count()

                if name_selector is not None:
                    appstr = f"App(id={resolved_id}, name={resolved_app.name}, site={resolved_app.site_id})"
                    if job_count == 0:
                        resolved_app.delete()
                        click.echo(f"Deleted {appstr}: there were no associated jobs.")
                    elif click.confirm(f"Really Delete {appstr}?? There are {job_count} Jobs that will be ERASED!"):
                        resolved_app.delete()
                        click.echo(f"Deleted App {resolved_id} ({name})")
                else:
                    num_apps += 1
                    num_jobs += job_count

            if all:
                if click.confirm(
                    f"Really DELETE {num_apps} apps and {num_jobs} jobs from site {site_selector}?? They will be ERASED!"
                ):
                    for name in app_list:
                        resolved_app = qs.get(name=name)
                        resolved_app.delete()
                    click.echo(f"Deleted {num_apps} apps and {num_jobs} jobs from site {site_selector}")
        else:
            click.echo("Found no apps to Delete")
