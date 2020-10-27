import yaml
from pathlib import Path
import click
from .utils import load_site_config
from balsam.config import ClientSettings
from balsam.api import App, Job


@click.group()
@click.pass_context
def job(ctx):
    """
    Create and monitor Balsam Jobs
    """
    ctx.obj = load_site_config()
    ClientSettings.load_from_home().build_client()


def list_to_dict(arg_list):
    return dict(arg.split("=") for arg in arg_list)


def validate_tags(ctx, param, value):
    try:
        return list_to_dict(value)
    except ValueError:
        raise click.BadParameter("needs to be in format KEY=VALUE")


def validate_app(ctx, param, value):
    site_id = ctx.obj.settings.site_id
    lookup = {"site_id": site_id}
    if value.isdigit():
        lookup["id"] = int(value)
    else:
        lookup["class_path"] = value
    try:
        app = App.objects.get(**lookup)
    except App.DoesNotExist:
        raise click.BadParameter(f"No App matching criteria {lookup}")
    return app


def validate_set(all_params, required_params, provided_params):
    missing_params = required_params.difference(provided_params)
    extraneous_params = provided_params.difference(all_params)
    if missing_params:
        raise click.BadParameter(f"Missing required: {missing_params}")
    if extraneous_params:
        raise click.BadParameter(f"Extraneous values: {extraneous_params}")


def validate_parameters(ctx, param, value):
    app = ctx.params["app"]
    params = validate_tags(ctx, param, value)

    all_params = set(app.parameters.keys())
    required_params = {k for k in all_params if app.parameters[k].required}
    provided = set(params.keys())
    validate_set(all_params, required_params, provided)
    return params


def validate_transfers(ctx, param, value):
    transfers = validate_tags(ctx, param, value)
    app = ctx.params["app"]
    all_transfers = set(app.transfers.keys())
    required_transfers = {k for k in all_transfers if app.transfers[k].required}
    provided = set(transfers.keys())
    validate_set(all_transfers, required_transfers, provided)

    for name in transfers:
        try:
            loc, path = transfers[name].split(":")
        except ValueError:
            raise click.BadParameter(
                f"Transfers must take the form LOCATION_ALIAS:PATH"
            )
        transfers[name] = {"location_alias": loc, "path": path}
    return transfers


def validate_parents(ctx, param, value):
    parent_ids = value
    if not parent_ids:
        return []
    jobs = list(Job.objects.filter(id=parent_ids))
    if len(jobs) < len(parent_ids):
        job_ids = [j.id for j in jobs]
        missing_ids = [i for i in parent_ids if i not in job_ids]
        raise click.BadParameter(f"Could not find parent job ids {missing_ids}")
    return parent_ids


@job.command()
@click.option("-w", "--workdir", required=True, type=str)
@click.option("-a", "--app", required=True, type=str, callback=validate_app)
@click.option("-t", "--tag", "tags", multiple=True, type=str, callback=validate_tags)
@click.option(
    "-p", "--param", "parameters", multiple=True, type=str, callback=validate_parameters
)
@click.option("-n", "--num-nodes", default=1, type=int)
@click.option("-rpn", "--ranks-per-node", default=1, type=int)
@click.option("-tpr", "--threads-per-rank", default=1, type=int)
@click.option("-tpc", "--threads-per-core", default=1, type=int)
@click.option("-g", "--gpus-per-rank", default=0, type=float)
@click.option("-npc", "--node-packing-count", default=1, type=int)
@click.option(
    "-lp",
    "--launch-param",
    "launch_params",
    multiple=True,
    type=str,
    callback=validate_tags,
)
@click.option("-t", "--wall-time-min", default=1, type=int)
@click.option(
    "-pid",
    "--parent-id",
    "parent_ids",
    multiple=True,
    type=int,
    callback=validate_parents,
)
@click.option(
    "-s",
    "--stage-data",
    "transfers",
    multiple=True,
    type=str,
    callback=validate_transfers,
)
def create(
    workdir,
    app,
    tags,
    parameters,
    num_nodes,
    ranks_per_node,
    threads_per_rank,
    threads_per_core,
    gpus_per_rank,
    node_packing_count,
    launch_params,
    wall_time_min,
    parent_ids,
    transfers,
):
    """
    Add a new Balsam Job to run an App at this Site
    """
    if Path(workdir).is_absolute():
        raise click.BadParameter(
            f"workdir must be a relative path: cannot start with '/'"
        )
    job = Job(
        workdir=workdir,
        app_id=app.id,
        tags=tags,
        parameters=parameters,
        num_nodes=num_nodes,
        ranks_per_node=ranks_per_node,
        threads_per_rank=threads_per_rank,
        threads_per_core=threads_per_core,
        gpus_per_rank=gpus_per_rank,
        node_packing_count=node_packing_count,
        launch_params=launch_params,
        wall_time_min=wall_time_min,
        parent_ids=parent_ids,
        transfers=transfers,
    )
    print("CREATE JOB:")
    print(yaml.dump(job.display_dict(), indent=4))


@job.command()
def ls(tags, state, workdir, verbose, history):
    """
    List Balsam Jobs
    """
    pass


@job.command()
def rm(id, tags):
    """
    Remove Jobs
    """
    pass
