from typing import Dict, List, cast

import click
import yaml

from balsam.schemas import BatchJobPartition, JobMode

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
@click.option("-h", "--history", is_flag=True, default=False)
@click.option("--site", "site_selector", default="")
def ls(history: bool, site_selector: str) -> None:
    """
    List BatchJobs

    1) View current BatchJobs

        balsam queue ls

    2) View historical BatchJobs at all sites

        balsam queue ls --history --site all
    """
    client = load_client()
    BatchJob = client.BatchJob
    qs = filter_by_sites(BatchJob.objects.all(), site_selector)
    if not history:
        qs = qs.filter(state=["pending_submission", "queued", "running", "pending_deletion"])

    jobs = [j.display_dict() for j in qs]
    sites = {site.id: site for site in client.Site.objects.all()}
    for job in jobs:
        site = sites[job["site_id"]]
        path_str = site.path.as_posix()
        if len(path_str) > 27:
            path_str = "..." + path_str[-27:]
        job["site"] = f"{site.hostname}:{path_str}"

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
