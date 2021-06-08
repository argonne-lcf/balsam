import getpass
import logging
import os
import stat
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Type

from balsam.platform.scheduler import (
    SchedulerDeleteError,
    SchedulerError,
    SchedulerNonZeroReturnCode,
    SchedulerSubmitError,
)
from balsam.schemas import AllowedQueue, BatchJobState, SchedulerJobStatus
from balsam.site import ScriptTemplate

from .service_base import BalsamService

if TYPE_CHECKING:
    from balsam._api.models import BatchJob
    from balsam.client import RESTClient
    from balsam.platform.scheduler import SchedulerInterface  # noqa: F401

logger = logging.getLogger(__name__)


class SchedulerService(BalsamService):
    def __init__(
        self,
        client: "RESTClient",
        site_id: int,
        scheduler_class: Type["SchedulerInterface"],
        sync_period: int,
        allowed_queues: Dict[str, AllowedQueue],
        allowed_projects: List[str],
        optional_batch_job_params: Dict[str, str],
        job_template_path: Path,
        submit_directory: Path,
        filter_tags: Dict[str, str],
    ) -> None:
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

    def fail_submit(self, job: "BatchJob", msg: str) -> None:
        job.state = BatchJobState.submit_failed
        job.status_info = {**(job.status_info or {}), "error": msg}
        logger.error(f"Submit failed for BatchJob {job.id}: {msg}")

    def submit_launch(self, job: "BatchJob", scheduler_jobs: Dict[int, SchedulerJobStatus]) -> None:
        try:
            job.validate(
                self.allowed_queues,
                self.allowed_projects,
                self.optional_batch_job_params,
            )
        except ValueError as e:
            return self.fail_submit(job, str(e))

        num_queued = len([j for j in scheduler_jobs.values() if j.queue == job.queue])
        queue = self.allowed_queues[job.queue]
        if num_queued >= queue.max_queued_jobs:
            return self.fail_submit(job, f"Exceeded max {queue.max_queued_jobs} jobs in queue {job.queue}")

        script = self.job_template.render(
            project=job.project,
            queue=job.queue,
            num_nodes=job.num_nodes,
            wall_time_min=job.wall_time_min,
            job_mode=job.job_mode,
            filter_tags=job.filter_tags,
            partitions=job.partitions_to_cli_args(),
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
            assert scheduler_id is not None
            job.scheduler_id = scheduler_id
            job.state = BatchJobState.queued
            job.status_info = {
                **(job.status_info or {}),
                "submit_script": script_path.as_posix(),
                "submit_time": str(datetime.utcnow()),
            }
            logger.info(f"Submit OK: {job}")

    def run_cycle(self) -> None:
        BatchJob = self.client.BatchJob
        api_jobs = list(
            BatchJob.objects.filter(
                site_id=self.site_id,
                state=["pending_submission", "queued", "running", "pending_deletion"],
            )
        )
        logger.debug(f"Fetched API BatchJobs: {[(j.id, j.state) for j in api_jobs]}")
        try:
            scheduler_jobs = self.scheduler.get_statuses(user=self.username)
        except SchedulerNonZeroReturnCode as exc:
            logger.error(f"scheduler.get_statuses nonzero return: {exc}")
            return

        for job in api_jobs:
            if job.state == "finished":
                continue
            if job.state == "pending_submission":
                if all(item in job.filter_tags.items() for item in self.filter_tags.items()):
                    self.submit_launch(job, scheduler_jobs)
                else:
                    logger.debug(
                        f"Will not submit batchjob {job.id}: does not match "
                        f"the current filter_tags criteria: {self.filter_tags}"
                    )
            elif job.state == "pending_deletion" and job.scheduler_id in scheduler_jobs:
                logger.info(f"Performing queue-deletion of batch job {job.id}")
                try:
                    self.scheduler.delete_job(job.scheduler_id)
                except SchedulerDeleteError as exc:
                    logger.warning(f"Failed to delete job {job.scheduler_id}: {exc}")
            elif job.scheduler_id not in scheduler_jobs:
                logger.info(
                    f"batch job {job.id}: scheduler_id {job.scheduler_id} no longer in queue statuses: finished"
                )
                job.state = BatchJobState.finished
                assert job.scheduler_id is not None
                assert job.status_info is not None
                job_log = self.scheduler.parse_logs(job.scheduler_id, job.status_info.get("submit_script", None))
                start_time = job_log.start_time
                end_time = job_log.end_time
                if start_time:
                    job.start_time = start_time
                if end_time:
                    job.end_time = end_time
            elif job.state != scheduler_jobs[job.scheduler_id].state:
                job.state = scheduler_jobs[job.scheduler_id].state
                logger.info(f"Job {job.id} (sched_id {job.scheduler_id}) advanced to state {job.state}")
        BatchJob.objects.bulk_update(api_jobs)
        self.update_site_info(scheduler_jobs)

    def update_site_info(self, scheduler_jobs: Dict[int, SchedulerJobStatus]) -> None:
        """Update on Site from nodelist & qstat"""
        # TODO: Periodically update Site nodelist; queues from here:
        site = self.client.Site.objects.get(id=self.site_id)
        try:
            site.backfill_windows = self.scheduler.get_backfill_windows()
        except SchedulerError as e:
            logger.warning(f"get_backfill_windows() error: {e}")
            logger.warning("Clearing site backfill windows; could not obtain")
            site.backfill_windows = {}
        site.queued_jobs = scheduler_jobs
        site.save()
        logger.debug(f"Updated Site info: {site.display_dict()}")

    def cleanup(self) -> None:
        logger.info("SchedulerService exiting")
