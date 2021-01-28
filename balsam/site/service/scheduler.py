from datetime import datetime
import getpass
import logging
import os
import stat
from balsam.site import ScriptTemplate
from balsam.platform.scheduler import SchedulerSubmitError
from .service_base import BalsamService
from balsam.cmdline.utils import partitions_to_cli_args


logger = logging.getLogger(__name__)


def validate_batch_job(
    job, allowed_queues, allowed_projects, optional_batch_job_params
):
    if job.queue not in allowed_queues:
        raise ValueError(
            f"Unknown queue {job.queue} " f"(known: {list(allowed_queues.keys())})"
        )
    queue = allowed_queues[job.queue]
    if job.num_nodes > queue.max_nodes:
        raise ValueError(
            f"{job.num_nodes} exceeds queue max num_nodes {queue.max_nodes}"
        )
    if job.num_nodes < 1:
        raise ValueError("Job size must be at least 1 node")
    if job.wall_time_min > queue.max_walltime:
        raise ValueError(
            f"{job.wall_time_min} exceeds queue max wall_time_min {queue.max_walltime}"
        )

    if job.project not in allowed_projects:
        raise ValueError(
            f"Unknown project {job.project} " f"(known: {allowed_projects})"
        )
    if job.partitions:
        if sum(part.num_nodes for part in job.partitions) != job.num_nodes:
            raise ValueError("Sum of partition sizes must equal batchjob num_nodes")

    extras = set(job.optional_params.keys())
    allowed_extras = set(optional_batch_job_params.keys())
    extraneous = extras.difference(allowed_extras)
    if extraneous:
        raise ValueError(
            f"Extraneous optional_params: {extraneous} "
            f"(allowed extras: {allowed_extras})"
        )


class SchedulerService(BalsamService):
    def __init__(
        self,
        client,
        site_id,
        scheduler_class,
        sync_period,
        allowed_queues,
        allowed_projects,
        optional_batch_job_params,
        job_template_path,
        submit_directory,
        filter_tags,
    ):
        super().__init__(client=client, service_period=sync_period)
        self.site_id = site_id
        self.scheduler = scheduler_class()
        self.allowed_queues = allowed_queues
        self.allowed_projects = allowed_projects
        self.optional_batch_job_params = optional_batch_job_params
        self.job_template = ScriptTemplate(job_template_path)
        self.submit_directory = submit_directory
        self.username = getpass.getuser()
        self.filter_tags = filter_tags
        logger.info(f"Initialized SchedulerService:\n{self.__dict__}")

    def fail_submit(self, job, msg):
        job.state = "submit_failed"
        job.status_info = {**job.status_info, "error": msg}
        logger.error(f"Submit failed for BatchJob {job.id}: {msg}")

    def submit_launch(self, job, scheduler_jobs):
        try:
            validate_batch_job(
                job,
                self.allowed_queues,
                self.allowed_projects,
                self.optional_batch_job_params,
            )
        except ValueError as e:
            return self.fail_submit(job, str(e))

        num_queued = len([j for j in scheduler_jobs.values() if j.queue == job.queue])
        queue = self.allowed_queues[job.queue]
        if num_queued >= queue.max_queued_jobs:
            return self.fail_submit(
                job, f"Exceeded max {queue.max_queued_jobs} jobs in queue {job.queue}"
            )

        script = self.job_template.render(
            project=job.project,
            queue=job.queue,
            num_nodes=job.num_nodes,
            wall_time_min=job.wall_time_min,
            job_mode=job.job_mode,
            filter_tags=job.filter_tags,
            partitions=partitions_to_cli_args(job.partitions),
            **job.optional_params,
        )

        script_path = self.submit_directory.joinpath(f"qlaunch{job.id:05d}.sh")
        with open(script_path, "w") as fp:
            fp.write(script)

        logger.info(f"Generated script for batch_job {job.id} in {script_path}")

        st = os.stat(script_path)
        os.chmod(script_path, st.st_mode | stat.S_IEXEC)

        try:
            scheduler_id = self.scheduler.submit(
                script_path,
                project=job.project,
                queue=job.queue,
                num_nodes=job.num_nodes,
                wall_time_min=job.wall_time_min,
                cwd=self.submit_directory,
            )
        except SchedulerSubmitError as e:
            return self.fail_submit(job, f"Scheduler submit error:\n{e}")
        else:
            job.scheduler_id = scheduler_id
            job.state = "queued"
            job.status_info = {
                **job.status_info,
                "submit_script": script_path,
                "submit_time": datetime.utcnow(),
            }
            logger.info(f"Submit OK: {job}")

    def run_cycle(self):
        BatchJob = self.client.BatchJob
        api_jobs = BatchJob.objects.filter(
            site_id=self.site_id,
            state=["pending_submission", "queued", "running", "pending_deletion"],
        )
        api_jobs = list(api_jobs)
        logger.info(f"Fetched API BatchJobs: {[(j.id, j.state) for j in api_jobs]}")
        scheduler_jobs = self.scheduler.get_statuses(user=self.username)

        for job in api_jobs:
            if job.state == "finished":
                continue
            if job.state == "pending_submission":
                if all(
                    item in job.filter_tags.items() for item in self.filter_tags.items()
                ):
                    self.submit_launch(job, scheduler_jobs)
                else:
                    logger.debug(
                        f"Will not submit batchjob {job.id}: does not match "
                        f"the current filter_tags criteria: {self.filter_tags}"
                    )
            elif job.state == "pending_deletion" and job.scheduler_id in scheduler_jobs:
                logger.info(f"Performing queue-deletion of batch job {job.id}")
                self.scheduler.delete_job(job.scheduler_id)
            elif job.scheduler_id not in scheduler_jobs:
                logger.info(
                    f"batch job {job.id}: scheduler_id {job.scheduler_id} no longer in queue statuses: finished"
                )
                job.state = "finished"
                job_log = self.scheduler.parse_logs(
                    job.scheduler_id, job.status_info.get("submit_script", None)
                )
                start_time = job_log.start_time
                end_time = job_log.end_time
                if start_time:
                    job.start_time = start_time
                if end_time:
                    job.end_time = end_time
            elif job.state != scheduler_jobs[job.scheduler_id].state:
                job.state = scheduler_jobs[job.scheduler_id].state
                logger.info(
                    f"Job {job.id} (sched_id {job.scheduler_id}) advanced to state {job.state}"
                )
        BatchJob.objects.bulk_update(api_jobs)
        self.update_site_info()

    def update_site_info(self):
        """Update on Site from nodelist & qstat"""
        # TODO: Periodically update Site nodelist; queues from here:
        site = self.client.Site.objects.get(id=self.site_id)
        site.backfill_windows = []
        site.num_nodes = 0
        site.queued_jobs = []
        site.save()
        logger.info(f"Updated Site info: {site.display_dict()}")

    def cleanup(self):
        logger.info("SchedulerService exiting")
