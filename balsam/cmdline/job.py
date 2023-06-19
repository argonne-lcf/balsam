from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Union, cast

import click
import yaml

from balsam.schemas import JobState, JobTransferItem

from .utils import filter_by_sites, load_client, table_print, validate_tags

if TYPE_CHECKING:
    from balsam._api.models import App, AppQuery, JobQuery
    from balsam.client import RESTClient  # noqa: F401


@click.group()
def job() -> None:
    """
    Create and monitor Balsam Jobs
    """
    pass


def validate_state(ctx: Any, param: Any, value: Union[None, str, JobState]) -> Union[None, JobState]:
    if value is None:
        return value
    if not JobState.is_valid(value):
        raise click.BadParameter(f"Invalid state {value}")
    return cast(JobState, value)


def fetch_app(app_qs: "AppQuery", app_str: str) -> "App":
    App = app_qs._manager._model_class
    lookup: Dict[str, Any]
    if app_str.isdigit():
        lookup = {"id": int(app_str)}
    else:
        lookup = {"name": app_str}
    try:
        app = app_qs.get(**lookup)
    except App.DoesNotExist:
        raise click.BadParameter(f"No App matching criteria {lookup}")
    return app


def validate_set(all_params: Set[str], required_params: Set[str], provided_params: Set[str]) -> None:
    missing_params = required_params.difference(provided_params)
    extraneous_params = provided_params.difference(all_params)
    if missing_params:
        raise click.BadParameter(f"Missing required parameters (-p): {missing_params}")
    if extraneous_params:
        raise click.BadParameter(f"Extraneous parameters (-p): {extraneous_params}")


def validate_parameters(parameters: List[str], app: "App") -> Dict[str, str]:
    params = validate_tags(None, None, parameters)
    all_params = set(app.parameters.keys())
    required_params = {k for k in all_params if app.parameters[k].required}
    provided = set(params.keys())
    validate_set(all_params, required_params, provided)
    return params


def validate_transfers(transfer_args: List[str], app: "App") -> Dict[str, JobTransferItem]:
    transfers = validate_tags(None, None, transfer_args)
    all_transfers = set(app.transfers.keys())
    required_transfers = {k for k in all_transfers if app.transfers[k].required}
    provided = set(transfers.keys())
    validate_set(all_transfers, required_transfers, provided)

    transfers_by_name: Dict[str, JobTransferItem] = {}
    for name in transfers:
        try:
            loc, path = transfers[name].split(":")
        except ValueError:
            raise click.BadParameter("Transfers must take the form LOCATION_ALIAS:PATH")
        transfers_by_name[name] = JobTransferItem(location_alias=loc, path=path)
    return transfers_by_name


def validate_parents(parent_ids: List[int], client: "RESTClient") -> None:
    if not parent_ids:
        return None
    jobs = list(client.Job.objects.filter(id=parent_ids))
    if len(jobs) < len(parent_ids):
        job_ids = [j.id for j in jobs]
        missing_ids = [i for i in parent_ids if i not in job_ids]
        raise click.BadParameter(f"Could not find parent job ids {missing_ids}")
    return None


@job.command()
@click.option("-w", "--workdir", required=True, type=str, help="Job directory (relative to data/)")
@click.option("-a", "--app", "app_str", required=True, type=str, help="App ID or name (module.ClassName)")
@click.option(
    "-tag", "--tag", "tags", multiple=True, type=str, callback=validate_tags, help="Job tags (--tag KEY=VALUE)"
)
@click.option(
    "-p",
    "--param",
    "parameters",
    multiple=True,
    type=str,
    help="App command template parameters (--param name=value)",
)
@click.option("-n", "--num-nodes", default=1, type=int, help="Number of compute nodes to run on", show_default=True)
@click.option("-rpn", "--ranks-per-node", default=1, type=int, help="MPI ranks per node", show_default=True)
@click.option("-tpr", "--threads-per-rank", default=1, type=int, help="Threads per process/rank", show_default=True)
@click.option("-tpc", "--threads-per-core", default=1, type=int, help="Threads per CPU core", show_default=True)
@click.option("-g", "--gpus-per-rank", default=0, type=float, help="GPUs per process/rank", show_default=True)
@click.option(
    "-npc", "--node-packing-count", default=1, type=int, help="Max concurrent runs per node", show_default=True
)
@click.option(
    "-lp",
    "--launch-param",
    "launch_params",
    multiple=True,
    type=str,
    callback=validate_tags,
    help="Pass-through parameters to MPI launcher (-lp name=value)",
)
@click.option("-t", "--wall-time-min", default=1, type=int)
@click.option(
    "-pid",
    "--parent-id",
    "parent_ids",
    multiple=True,
    type=int,
    help="Job dependencies given as one or many parent IDs",
    show_default=True,
)
@click.option(
    "-s",
    "--stage-data",
    "transfer_args",
    multiple=True,
    type=str,
    help="Transfer slots given as TRANSFER_SLOT=LOCATION_ALIAS:/path/to/file",
)
@click.option("-y", "--yes", "force_create", is_flag=True, default=False)
@click.option("--site", "site_selector", default="", help="Site ID or path fragment")
def create(
    workdir: str,
    app_str: str,
    tags: Dict[str, str],
    parameters: List[str],
    num_nodes: int,
    ranks_per_node: int,
    threads_per_rank: int,
    threads_per_core: int,
    gpus_per_rank: int,
    node_packing_count: int,
    launch_params: Dict[str, str],
    wall_time_min: int,
    parent_ids: List[int],
    transfer_args: List[str],
    site_selector: str,
    force_create: bool,
) -> None:
    """
    Add a new Balsam Job

    Examples:

    Create a Job in workdir "data/test/1" running app `demo.Hello` with parameter name="world!"

        balsam job create -w test/1 -a demo.Hello -p name="world!"
    """
    client: RESTClient = load_client()
    if Path(workdir).is_absolute():
        raise click.BadParameter("workdir must be a relative path: cannot start with '/'")

    app_qs = filter_by_sites(client.App.objects.all(), site_selector)
    app = fetch_app(app_qs, app_str)
    assert app.id is not None, "Could not resolve application ID"
    parameters_dict = validate_parameters(parameters, app)
    transfers = validate_transfers(transfer_args, app)
    validate_parents(parent_ids, client)

    job = client.Job(
        workdir=Path(workdir),
        app_id=app.id,
        tags=tags,
        parameters=parameters_dict,
        num_nodes=num_nodes,
        ranks_per_node=ranks_per_node,
        threads_per_rank=threads_per_rank,
        threads_per_core=threads_per_core,
        gpus_per_rank=gpus_per_rank,
        node_packing_count=node_packing_count,
        launch_params=launch_params,
        wall_time_min=wall_time_min,
        parent_ids=set(parent_ids),
        transfers=transfers,
    )
    click.echo(yaml.dump(job.display_dict(), sort_keys=False, indent=4))
    if force_create or click.confirm("Do you want to create this Job?"):
        job.save()
        click.echo(f"Added Job id={job.id}")


def count_by_state(job_qs: "JobQuery", verbose: bool) -> None:
    state_data: List[Dict[str, Any]] = []
    # job_qs
    for state in JobState:
        state_count = job_qs.filter(state=state).count()
        assert state_count is not None
        if state_count > 0 or verbose:
            state_dict = {"State": state.value, "Count": state_count}
            state_data.append(state_dict)

    table_print(state_data)


def list_verbose(job_qs: "JobQuery") -> None:
    for job in job_qs:
        click.echo(yaml.dump(job.display_dict(), sort_keys=False, indent=4))
        click.echo(f"deserialized parameters: {str(job.get_parameters())}")
        click.echo("---\n")


def list_table(job_qs: "JobQuery", client: "RESTClient") -> None:
    sites = {s.id: s for s in client.Site.objects.all()}
    apps = {a.id: a for a in client.App.objects.all()}
    data = []
    for j in job_qs:
        app = apps[j.app_id]
        site = sites[app.site_id]
        assert j.state is not None
        jdict = {
            "ID": j.id,
            "Site": f"{site.name}",
            "App": app.name,
            "Workdir": j.workdir.as_posix(),
            "State": j.state.value,
            "Tags": j.tags,
        }
        data.append(jdict)
    table_print(data)


@job.command()
@click.option("-t", "--tag", "tags", multiple=True, type=str, callback=validate_tags)
@click.option("-s", "--state", type=str, callback=validate_state)
@click.option("-ns", "--exclude-state", type=str, callback=validate_state)
@click.option("--id", type=str)
@click.option("--by-state", type=bool, default=False, is_flag=True)
@click.option("-w", "--workdir", type=str)
@click.option("-a", "--app", type=str)
@click.option("--site", "site_selector", default="")
@click.option("-v", "--verbose", is_flag=True)
def ls(
    tags: List[str],
    state: Optional[JobState],
    exclude_state: Optional[JobState],
    id: Optional[int],
    by_state: Optional[bool],
    app: Optional[str],
    workdir: Optional[str],
    verbose: bool,
    site_selector: str,
) -> None:
    """
    List Balsam Jobs

    1) Filter by Site ID or Path fragments (comma-separated)

        balsam job ls --site=123,my-cori-site

    2) Select Jobs by their tags

        balsam job ls --tag experiment=XPCS --tag system=H2O

    3) Select Jobs by their app

        balsam job ls --app flux_capacitance

    4) Select Jobs by their state

        balsam job ls --state JOB_FINISHED --tag system=H2O

    5) Summarize Jobs by their state

        balsam job ls --by-state [--tag system=H20]

    6) Select a specific job by ID

        balsam job ls --id [id]
    """
    client = load_client()
    job_qs = filter_by_sites(client.Job.objects.all(), site_selector)
    if app:
        app_qs = filter_by_sites(client.App.objects.all(), site_selector)
        appo = fetch_app(app_qs, app)
        job_qs = job_qs.filter(app_id=appo.id)
    if tags:
        job_qs = job_qs.filter(tags=tags)
    if state:
        job_qs = job_qs.filter(state=state)
    if exclude_state:
        job_qs = job_qs.filter(state__ne=exclude_state)
    if workdir:
        job_qs = job_qs.filter(workdir__contains=workdir)
    if id:
        job_qs = job_qs.filter(id=[id])

    if by_state:
        count_by_state(job_qs, verbose)
    elif verbose:
        list_verbose(job_qs)
    else:
        list_table(job_qs, client)


@job.command()
@click.option("-i", "--id", "job_ids", multiple=True, type=int)
@click.option("-t", "--tag", "tags", multiple=True, type=str, callback=validate_tags)
@click.option("-s", "--state", "state", type=str)
def modify(job_ids: List[int], tags: List[str], state: JobState) -> None:
    """
    Modify Jobs

    1) Modify Job State

        balsam job modify --id 41 --id 42 --id 43 -s RESTART_READY
    """
    client: RESTClient = load_client()
    jobs = client.Job.objects.all()
    if job_ids:
        jobs = jobs.filter(id=job_ids)
    elif tags:
        jobs = jobs.filter(tags=tags)
    else:
        raise click.BadParameter("Provide either list of Job ids or tags to delete")
    count = jobs.count()
    assert count is not None
    for job in jobs:
        job.state = state
        job.save()


@job.command()
@click.option("-i", "--id", "job_ids", multiple=True, type=int)
@click.option("-t", "--tag", "tags", multiple=True, type=str, callback=validate_tags)
@click.option("-y", "yes", is_flag=True, default=False)
@click.option("--all", is_flag=True, default=False)
def rm(job_ids: List[int], tags: List[str], yes: bool, all: bool) -> None:
    """
    Remove Jobs

    1) Remove Jobs by ID

        balsam job rm --id 41 --id 42 --id 43

    2) Remove Jobs by Tags

        balsam job rm --tag workflow=temp-test

    3) Remove all jobs (DANGER!)

        balsam job rm --all
    """
    client: RESTClient = load_client()
    jobs = client.Job.objects.all()
    if job_ids:
        jobs = jobs.filter(id=job_ids)
    elif tags:
        jobs = jobs.filter(tags=tags)
    elif all:
        click.echo("THIS WILL DELETE ALL JOBS! CAUTION!")
        pass
    else:
        raise click.BadParameter("Provide either list of Job ids or tags to delete")
    count = jobs.count()
    assert count is not None
    if count < 1:
        click.echo("No jobs match deletion query")
    elif yes or click.confirm(f"Really delete {count} jobs?"):
        jobs.delete()
