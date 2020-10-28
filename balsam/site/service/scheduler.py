from datetime import datetime
import getpass
import logging
import os
import stat
from balsam.platform.job_template import ScriptTemplate
from balsam.platform.scheduler import SchedulerSubmitError
from .service_base import BalsamService


logger = logging.getLogger(__name__)


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
        logger.info(f"Initialized SchedulerService:\n{self.__dict__}")

    def fail_submit(self, job, msg):
        job.state = "submit_failed"
        job.status_info["error"] = msg
        logger.error(f"Submit failed for BatchJob {job.id}: {msg}")

    def submit_launch(self, job, scheduler_jobs):
        if job.project not in self.allowed_projects:
            return self.fail_submit(job, f"Invalid project {job.project}")
        if job.queue not in self.allowed_queues:
            return self.fail_submit(job, f"Invalid queue {job.queue}")
        extraneous_params = set(job.optional_params).difference(
            self.optional_batch_job_params
        )
        if extraneous_params:
            return self.fail_submit(
                job, f"Invalid optional_params: {extraneous_params}"
            )
        queue = self.allowed_queues[job.queue]
        if job.num_nodes < 1 or job.num_nodes > queue.max_nodes:
            return self.fail_submit(job, f"Invalid num_nodes: {job.num_nodes}")
        if job.wall_time_min > queue.max_walltime:
            return self.fail_submit(job, f"Invalid wall_time_min: {job.wall_time_min}")
        num_queued = len([j for j in scheduler_jobs if j.queue == job.queue])
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
            **job.optional_params,
        )

        script_path = self.submit_directory.joinpath(f"qlaunch{job.id:05d}.sh")
        with open(script_path, "w") as fp:
            fp.write(script)

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
            job.status_info["submit_script"] = script_path
            job.status_info["submit_time"] = datetime.utcnow()
            logger.info(f"Submit OK: {job}")

    def run_cycle(self):
        BatchJob = self.client.BatchJob
        api_jobs = BatchJob.objects.filter(
            site_id=self.site_id, state__ne=["submit_failed", "finished"]
        )
        scheduler_jobs = self.scheduler.get_statuses(user=self.username)

        for job in api_jobs:
            if job.state == "pending_submission":
                self.submit_launch(job, scheduler_jobs)
            elif job.state == "pending_deletion" and job.scheduler_id in scheduler_jobs:
                self.scheduler.delete_job(job.scheduler_id)
            elif job.scheduler_id not in scheduler_jobs:
                job.state = "finished"
                job_log = self.scheduler.parse_logs(job.scheduler_id)
                start_time = job_log.get("start_time")
                end_time = job_log.get("end_time")
                if start_time:
                    job.start_time = start_time
                if end_time:
                    job.end_time = end_time
            elif job.state != scheduler_jobs[job.scheduler_id].state:
                job.state = scheduler_jobs[job.scheduler_id].state
        BatchJob.objects.bulk_update(api_jobs)
        self.update_site_info()

    def update_site_info(self):
        """Update on Site from nodelist & qstat"""
        site = self.client.Site.objects.get(id=self.site_id)
        site.backfill_windows = []
        site.num_nodes = 0
        site.queued_jobs = []
        site.save()

    def cleanup(self):
        logger.info("SchedulerService exiting")
