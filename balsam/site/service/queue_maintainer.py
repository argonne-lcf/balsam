import getpass
import logging

from .service_base import BalsamService

logger = logging.getLogger(__name__)


class QueueMaintainerService(BalsamService):
    def __init__(
        self,
        client,
        site_id,
        submit_period=60,
        submit_project="datascience",
        submit_queue="balsam",
        job_mode="mpi",
        filter_tags=None,
        num_queued_jobs=5,
        num_nodes=20,
        wall_time_min=127,
    ):
        super().__init__(client=client, service_period=submit_period)
        self.site_id = site_id
        self.project = submit_project
        self.submit_queue = submit_queue
        self.job_mode = job_mode
        self.filter_tags = filter_tags
        self.num_queued_jobs = num_queued_jobs
        self.num_nodes = num_nodes
        self.wall_time_min = wall_time_min
        self.username = getpass.getuser()
        logger.info(f"Initialized QueueMaintainerService:\n{self.__dict__}")

    def get_next_submission(self):
        return {
            "project": self.project,
            "queue": self.submit_queue,
            "job_mode": self.job_mode,
            "filter_tags": self.filter_tags,
            "num_nodes": self.num_nodes,
            "wall_time_min": self.wall_time_min,
            "site_id": self.site_id,
        }

    def run_cycle(self):
        num_current = self.client.BatchJob.objects.filter(
            site_id=self.site_id,
            queue=self.submit_queue,
            state=["pending_submission", "queued", "running", "pending_deletion"],
        ).count()
        logger.debug(f"{num_current} currently active BatchJobs")
        if num_current < self.num_queued_jobs:
            sub = self.get_next_submission()
            new_job = self.client.BatchJob(**sub)
            new_job.save()
            logger.info(f"Submitted new BatchJob: {new_job}")

    def cleanup(self):
        logger.info("Exiting QueueMaintainer service")
