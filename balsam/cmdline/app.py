import click
from balsam.config import BalsamComponentFactory, ClientSettings
from balsam.api import App, AppBackend, Job
from balsam.site import app_template, ApplicationDefinition


def get_factory():
    try:
        cf = BalsamComponentFactory()
    except ValueError:
        raise click.BadParameter(
            "Cannot alter apps outside the scope of a Balsam Site. "
            "Please navigate into a Balsam site directory, or set "
            "BALSAM_SITE_PATH"
        )
    return cf


@click.group()
def app():
    """
    Sync and manage Balsam applications
    """
    pass


@app.command()
@click.option(
    "-n", "--name", required=True, prompt="Application Name (of the form MODULE.CLASS)"
)
@click.option(
    "-c",
    "--command-template",
    required=True,
    prompt="Application Template [e.g. 'echo Hello {{ name }}!']",
)
@click.option("-d", "--description", default="Application description")
def create(name, command_template, description):
    """
    Create a new Balsam AppBackend file in the current Site.

    The App file is generated according to a template; feel free to
    write app files without using this command.  You can also define
    several Apps per file.

    Example:

        balsam app create --name demo.Hello --command-template 'echo hello, {{name}}!'
    """
    cf = get_factory()

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
    click.echo(f"Created {app_file}")


@app.command()
@click.argument("app-class-name")
@click.option("-n", "--name")
def sync(app_class_name, name):
    """
    Sync a local App with Balsam
    """
    cf = get_factory()
    ClientSettings.load_from_home().build_client()

    app_class = ApplicationDefinition.load_app_class(cf.apps_path, app_class_name)
    app_data = app_class.as_dict()
    name = name or app_data["name"]

    new_backend = AppBackend(site=cf.settings.site_id, class_name=app_class_name)
    new_app = App(
        name=name,
        description=app_data["description"],
        parameters=app_data["parameters"],
        backends=[new_backend],
    )

    try:
        existing = App.objects.get(name=name)
    except App.DoesNotExist:
        new_app.save()
        click.echo(f"Created new App {new_app.pk}")
    else:
        existing.name = new_app.name
        existing.description = new_app.description
        existing.parameters = new_app.parameters
        backends = {b.site: b for b in existing.backends}
        backends[new_backend.site] = new_backend
        existing.backends = list(backends.values())
        existing.save()
        click.echo(f"Updated existing App {existing.pk}")


@app.command()
@click.option("-n", "--name", required=True)
def rm(name):
    """
    Remove an App by name
    """
    get_factory()
    ClientSettings.load_from_home().build_client()
    try:
        existing = App.objects.get(name=name)
    except App.DoesNotExist:
        raise click.BadParameter(f"Could not find an App named {name}")
    else:
        job_count = Job.objects.filter(app_id=existing.pk).count()
        if click.confirm(
            f"Really delete App {name}?  This will wipe out {job_count} associated Jobs!"
        ):
            existing.delete()
            click.echo(f"Deleted App {existing.name}")


@app.command()
def ls():
    """
    List my Apps
    """
    ClientSettings.load_from_home().build_client()
    for app in App.objects.all():
        backends = "\n".join(
            f" --> {b.class_name}@{b.site_hostname}:{b.site_path}" for b in app.backends
        )
        print(f"{app.pk} {app.name}")
        print(f"Parameters: {app.parameters}")
        print(backends)


@app.command()
@click.argument("app-names", nargs=-1)
@click.option("-n", "--name", required=True)
def merge(app_names, name):
    """
    Merge two or more Apps into an AppExchange

    When Jobs are created with an AppExchange, they may run at more than one
    Balsam site.

    Example:

        balsam app merge --name MyExchange simulation_theta simulation_cori
    """
    if len(app_names) < 2:
        raise click.BadParameter("Give at least two App names to merge")

    ClientSettings.load_from_home().build_client()
    apps = list(App.objects.filter(name__in=app_names))
    if len(app_names) != len(apps):
        found_set = set(a.name for a in apps)
        missing_set = set(app_names).difference(found_set)
        raise click.BadParameter(
            f"Could not find apps with the following names: {missing_set}"
        )

    merged = App.objects.merge(apps, name=name)
    click.echo(f"Created new merged App {merged.name}")
    backends = "\n".join(
        f" --> {b.class_name}@{b.site_hostname}:{b.site_path}" for b in merged.backends
    )
    click.echo(backends)


@app.command()
@click.argument("old-name")
@click.argument("new-name")
def rename(old_name, new_name):
    """
    Change name of an App
    """
    ClientSettings.load_from_home().build_client()
    try:
        app = App.objects.get(name=old_name)
    except App.DoesNotExist:
        raise click.BadParameter(f"No app with name {old_name}")
    else:
        app.name = new_name
        app.save()
        click.echo(f"Renamed app {old_name} --> {new_name}")
