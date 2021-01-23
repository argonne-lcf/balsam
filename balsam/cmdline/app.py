import yaml
from pathlib import Path
import click
from .utils import load_site_config
from balsam.config import ClientSettings
from balsam.site import app_template, ApplicationDefinition
from balsam.site.app import load_module, find_app_classes


def load_apps(apps_path):
    """
    Fetch all ApplicationDefinitions and their local modification times
    Returns two dicts keyed by module name
    """
    app_files = list(Path(apps_path).glob("*.py"))
    mtimes = {}
    app_classes = {}
    for fname in app_files:
        module_name = fname.with_suffix("").name
        mtimes[module_name] = fname.stat().st_mtime
        module = load_module(fname)
        app_classes[module_name] = find_app_classes(module)
    return app_classes, mtimes


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
        site_id=cf.settings.site_id,
        class_path=name,
        last_modified=mtime,
        **app_cls.as_dict(),
    )


def sync_app(client, app_class, class_path, mtime, registered_app, site_id):
    # App not in DB: create it
    if registered_app is None:
        app = client.App.objects.create(
            site_id=site_id,
            class_path=class_path,
            last_modified=mtime,
            **app_class.as_dict(),
        )
        click.echo(f"CREATED    {class_path} (app_id={app.id})")
    # App out of date; update it:
    elif registered_app.last_modified is None or registered_app.last_modified < mtime:
        for k, v in app_class.as_dict().items():
            setattr(registered_app, k, v)
        registered_app.last_modified = mtime
        registered_app.save()
        click.echo(f"UPDATED         {class_path} (app_id={registered_app.id})")
    else:
        click.echo(f"UP-TO-DATE      {class_path} (app_id={registered_app.id})")
    # Otherwise, app is up to date :)
    return


def app_deletion_prompt(client, app):
    job_count = client.Job.objects.filter(app_id=app.id).count()
    click.echo(f"DELETED/RENAMED {app.class_path} (app_id={app.id})")
    click.echo(f"   --> You either renamed this ApplicationDefinition or deleted it.")
    click.echo(f"   --> There are {job_count} Jobs associated with this App")
    delete = click.confirm(
        f"  --> Do you wish to unregister this App (this will ERASE {job_count} jobs!)"
    )
    if delete:
        app.delete()
        click.echo(f"  --> Deleted.")
    else:
        click.echo(
            f"  --> App not deleted. If you meant to rename it, please update the class_path in the API."
        )


@app.command()
def sync():
    """
    Sync local ApplicationDefinitions with Balsam
    """
    cf = load_site_config()
    client = cf.client

    registered_apps = list(client.App.objects.filter(site_id=cf.settings.site_id))
    app_classes, mtimes = load_apps(cf.apps_path)

    for module_name, app_class_list in app_classes.items():
        for app_class in app_class_list:
            class_path = f"{module_name}.{app_class.__name__}"
            registered_app = next(
                (a for a in registered_apps if a.class_path == class_path), None
            )
            sync_app(
                client,
                app_class,
                class_path,
                mtimes[module_name],
                registered_app,
                cf.settings.site_id,
            )
            if registered_app is not None:
                registered_apps.remove(registered_app)

    # Remaining registered_apps are no longer in the apps_path
    # They could have been deleted or renamed
    for app in registered_apps:
        app_deletion_prompt(client, app)


@app.command()
def ls():
    """
    List my Apps
    """
    client = ClientSettings.load_from_home().build_client()
    reprs = [
        yaml.dump(app.display_dict(), sort_keys=False, indent=4)
        for app in client.App.objects.all()
    ]
    print(*reprs, sep="\n----\n")
