from typing import Dict, List, cast

import click
import yaml

from balsam.schemas import BatchJobPartition, BatchJobState, JobMode

from .utils import filter_by_sites, load_client, load_site_from_selector, validate_partitions, validate_tags


@click.group()
def queue() -> None:
    """
    Submit and monitor BatchJobs (queued launcher pilots)
    """
    pass


@queue.command()
@click.option("-n", "--num-nodes", required=True, type=int)
@click.option("-t", "--wall-time-min", required=True, type=int)
@click.option("-q", "--queue", required=True)
@click.option("-A", "--project", required=True)
@click.option("-j", "--job-mode", type=click.Choice(["serial", "mpi"]), required=True)
@click.option("-tag", "--tag", "filter_tags", multiple=True, callback=validate_tags)
@click.option("-p", "--part", "partitions", multiple=True, callback=validate_partitions)
@click.option("-x", "--extra-param", "optional_params", multiple=True, callback=validate_tags)
@click.option("--site", "site_selector", default="")
def submit(
    num_nodes: int,
    wall_time_min: int,
    queue: str,
    project: str,
    job_mode: str,
    filter_tags: Dict[str, str],
    partitions: List[BatchJobPartition],
    optional_params: Dict[str, str],
    site_selector: str,
) -> None:
    """
    Submit a new BatchJob to the Site scheduler.

    1) Request 2 nodes to run all Jobs

        balsam queue submit -n 2 -t 60  -q default -A MyAllocation -j mpi

    2) Request 2 nodes to run only Jobs with certain tags

        balsam queue submit -n 2 -t 60  -q default -A alloc -j mpi -tag experiment=foo
    """
    client = load_client()
    BatchJob = client.BatchJob
    Site = client.Site

    try:
        site = load_site_from_selector(site_selector)
    except Site.DoesNotExist:
        raise click.BadParameter(f"No site matching --site {site_selector}")
    except Site.MultipleObjectsReturned:
        raise click.BadParameter(f"More than one site matching --site {site_selector}")
    assert site.id is not None

    job = BatchJob(
        site_id=site.id,
        num_nodes=num_nodes,
        wall_time_min=wall_time_min,
        queue=queue,
        project=project,
        job_mode=cast(JobMode, job_mode),
        optional_params=optional_params,
        filter_tags=filter_tags,
        partitions=partitions,
    )
    try:
        job.validate(
            site.allowed_queues,
            site.allowed_projects,
            site.optional_batch_job_params,
        )
    except ValueError as e:
        raise click.BadArgumentUsage(str(e))
    job.save()
    print(yaml.dump(job.display_dict(), sort_keys=False, indent=4))


@queue.command()
@click.option("-n", "--num", default=0, type=int)
@click.option("-h", "--history", is_flag=True, default=False)
@click.option("-v", "--verbose", is_flag=True, default=False)
@click.option("--site", "site_selector", default="")
@click.option("--scheduler_id", "scheduler_id", type=int, default=None)
def ls(history: bool, verbose: bool, num: int, site_selector: str, scheduler_id: int) -> None:
    """
    List BatchJobs

    1) View current BatchJobs

        balsam queue ls

    2) View historical BatchJobs at all sites

        balsam queue ls --history --site all

    3) View verbose record for BatchJob with scheduler id

        balsam queue ls --scheduler_id 12345 -v

    4) View the last n BatchJobs

        balsam queue ls --num n

    """
    client = load_client()
    BatchJob = client.BatchJob
    qs = filter_by_sites(BatchJob.objects.all(), site_selector)

    if not history and scheduler_id is None and num == 0:
        qs = qs.filter(state=["pending_submission", "queued", "running", "pending_deletion"])
        if len(qs) == 0:
            click.echo("No active batch jobs.  Use --history option to list completed batch jobs.")

    if scheduler_id is not None:
        qs = qs.filter(scheduler_id=scheduler_id)

    jobs = [j.display_dict() for j in qs]
    if not history and num > 0 and scheduler_id is None:
        click.echo(f"Displaying records for last {num} Batch Jobs")
        jobs = jobs[-num:]

    if verbose:
        for j in jobs:
            click.echo(j)
    else:
        sites = {site.id: site for site in client.Site.objects.all()}
        for job in jobs:
            site = sites[job["site_id"]]
            path_str = site.path.as_posix()
            if len(path_str) > 27:
                path_str = "..." + path_str[-27:]
            job["site"] = f"{site.name}"

        fields = [
            "id",
            "site",
            "scheduler_id",
            "state",
            "filter_tags",
            "project",
            "queue",
            "num_nodes",
            "wall_time_min",
            "job_mode",
        ]
        rows = [[str(j[field]) for field in fields] for j in jobs]

        col_widths = [len(f) for f in fields]
        for row in rows:
            for col_idx, width in enumerate(col_widths):
                col_widths[col_idx] = max(width, len(row[col_idx]))

        for i, field in enumerate(fields):
            fields[i] = field.rjust(col_widths[i] + 1)

        print(*fields)
        for row in rows:
            for i, col in enumerate(row):
                row[i] = col.rjust(col_widths[i] + 1)
            print(*row)


@queue.command()
@click.argument("scheduler_id", type=int)
def term(scheduler_id: int) -> None:
    """
    Terminate a BatchJob by scheduler id
    """
    client = load_client()
    BatchJob = client.BatchJob

    bjob = BatchJob.objects.get(scheduler_id=scheduler_id)
    if bjob.state in [BatchJobState.queued, BatchJobState.running]:
        bjob.state = BatchJobState.pending_deletion
        bjob.save()
        click.echo("Marked BatchJob for deletion")
    else:
        click.echo("BatchJob is not queued or running")
