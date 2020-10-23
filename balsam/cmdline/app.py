import click
from .utils import load_site_config
from balsam.config import ClientSettings
from balsam.api import App
from balsam.site import app_template, ApplicationDefinition


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
    Create a new Balsam App in the current Site.

    The App file is generated according to a template; feel free to
    write app files without using this command.  You can also define
    several Apps per file.

    Example:

        balsam app create --name demo.Hello --command-template 'echo hello, {{name}}!'
    """
    cf = load_site_config()
    ClientSettings.load_from_home().build_client()

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

    click.echo(f"Created App {cls_name} in {app_file}")
    app_cls = ApplicationDefinition.load_app_class(cf.apps_path, name)

    App.objects.create(
        site_id=cf.settings.site_id, class_path=name, **app_cls.as_dict(),
    )


@app.command()
@click.argument("app-class-name")
def sync(app_class_name):
    """
    Sync a local App with Balsam
    """
    cf = load_site_config()
    ClientSettings.load_from_home().build_client()

    registered_apps = list(App.objects.filter(site_id=cf.settings.site_id))

    app_files = list(cf.apps_path.glob("*.py"))
    file_mtimes = {
        fname.with_suffix("").name: fname.stat().st_mtime for fname in app_files
    }
    print(registered_apps, file_mtimes)

    # Load full list of App classes from each file: mapping App to mtime
    # For Apps not in DB, create
    # For Apps with mtime newer than in DB, update the App
    # For Apps with mtime matching DB mtime, do nothing (same)

    # For Apps in DB:
    # If DB App not in Site apps, prompt user: remove it from DB (it was deleted or renamed)
    #   WARN that this will destroy COUNT jobs associated to the app

    app_class = ApplicationDefinition.load_app_class(cf.apps_path, app_class_name)
    app_data = app_class.as_dict()

    new_app = App(site_id=cf.settings.site_id, class_path=app_class_name, **app_data)

    try:
        existing = App.objects.get(
            site_id=cf.settings.site_id, class_path=app_class_name
        )
    except App.DoesNotExist:
        new_app.save()
        click.echo(f"Created new App {new_app.id}")
    else:
        existing.description = new_app.description
        existing.parameters = new_app.parameters
        existing.transfers = new_app.transfers
        existing.save()
        click.echo(f"Updated existing App {existing.pk}")


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
