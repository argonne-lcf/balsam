import click
import yaml
from .utils import load_site_config, validate_tags, validate_partitions
from balsam.site.service.scheduler import validate_batch_job


@click.group()
@click.pass_context
def queue(ctx):
    """
    Submit and monitor queued launcher jobs
    """
    ctx.obj = load_site_config()


@queue.command()
@click.option("-n", "--num-nodes", required=True, type=int)
@click.option("-t", "--wall-time-min", required=True, type=int)
@click.option("-q", "--queue", required=True)
@click.option("-A", "--project", required=True)
@click.option("-j", "--job-mode", type=click.Choice(["serial", "mpi"]), required=True)
@click.option("-tag", "--tag", "filter_tags", multiple=True, callback=validate_tags)
@click.option("-p", "--part", "partitions", multiple=True, callback=validate_partitions)
@click.option(
    "-x", "--extra-param", "optional_params", multiple=True, callback=validate_tags
)
@click.pass_context
def submit(
    ctx,
    num_nodes,
    wall_time_min,
    queue,
    project,
    job_mode,
    filter_tags,
    partitions,
    optional_params,
):
    BatchJob = ctx.obj.client.BatchJob
    settings = ctx.obj.settings
    job = BatchJob(
        site_id=settings.site_id,
        num_nodes=num_nodes,
        wall_time_min=wall_time_min,
        queue=queue,
        project=project,
        job_mode=job_mode,
        optional_params=optional_params,
        filter_tags=filter_tags,
        partitions=partitions,
    )
    try:
        validate_batch_job(
            job,
            settings.scheduler.allowed_queues,
            settings.scheduler.allowed_projects,
            settings.scheduler.optional_batch_job_params,
        )
    except ValueError as e:
        raise click.BadArgumentUsage(str(e))
    job.save()
    print(yaml.dump(job.display_dict(), sort_keys=False, indent=4))


@queue.command()
@click.pass_context
def ls(ctx):
    BatchJob = ctx.obj.client.BatchJob
    site_id = ctx.obj.settings.site_id

    jobs = [j.display_dict() for j in BatchJob.objects.filter(site_id=site_id)]
    fields = [
        "id",
        "scheduler_id",
        "state",
        "filter_tags",
        "project",
        "queue",
        "num_nodes",
        "wall_time_min",
        "job_mode",
        # "optional_params",
        # "status_info",
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
