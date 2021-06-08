import socket
from pathlib import Path

import click
import yaml

from balsam.config import ClientSettings
from balsam.site import ApplicationDefinition, app_template
from balsam.site.app import sync_apps

from .utils import check_killable, filter_by_sites, kill_site, load_site_config, start_site


@click.group()
def app() -> None:
    """
    Sync and manage Balsam applications
    """
    pass


@app.command()
@click.option("-n", "--name", required=True, prompt="Application Name (of the form MODULE.CLASS)")
@click.option(
    "-c",
    "--command-template",
    required=True,
    prompt="Application Template [e.g. 'echo Hello {{ name }}!']",
)
@click.option("-d", "--description", default="Application description")
def create(name: str, command_template: str, description: str) -> None:
    """
    Create a new Balsam App in the current Site.

    The App file is generated according to a template to get you started.
    Feel free to write your own app files without using this command.
    You can also define several Apps per file.  Run `balsam app sync`
    to synchronize changes in your ApplicationDefinitions with the service.

    Example:

        balsam app create --name demo.Hello --command-template 'echo hello, {{name}}!'
    """
    cf = load_site_config()
    client = cf.client

    try:
        module_name, cls_name = name.split(".")
        assert module_name.isidentifier()
        assert cls_name.isidentifier()
    except (ValueError, AssertionError):
        raise click.BadParameter("name must take the form: MODULE.CLASS")

    app_body = app_template.render(
        dict(
            cls_name=cls_name,
            command_template=command_template,
            description=description,
        )
    )
    app_file = cf.apps_path.joinpath(module_name + ".py")
    if app_file.exists():
        raise click.BadParameter(
            f"Will not overwrite {app_file}. "
            "Instead of using the CLI, just append your ApplicationDefinition to the same file."
        )
    with open(app_file, "w") as fp:
        fp.write(app_body)
    mtime = Path(app_file).stat().st_mtime

    click.echo(f"Created App {cls_name} in {app_file}")
    app_cls = ApplicationDefinition.load_app_class(cf.apps_path, name)

    client.App.objects.create(
        site_id=cf.site_id,
        class_path=name,
        last_modified=mtime,
        **app_cls.as_dict(),
    )


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
            site_str = f"{site.hostname}:{site.path}"
            click.echo(f"{a.id:>5d}   {a.class_path:>18s}   {site_str:<20s}")
