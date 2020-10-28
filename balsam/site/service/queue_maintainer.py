import getpass
from .service_base import BalsamService
from balsam.api import Manager, BatchJob
import logging

logger = logging.getLogger(__name__)


class QueueMaintainerService(BalsamService):
    def __init__(
        self,
        client,
        site_id,
        scheduler_class,
        log_conf,
        service_period=60,
        submit_project="datascience",
        submit_queue="balsam",
        job_mode="mpi",
        filter_tags=None,
        num_queued_jobs=5,
        num_nodes=20,
        wall_time_min=127,
    ):
        super().__init__(log_conf=log_conf, service_period=service_period)
        Manager.set_client(client)
        self.site_id = site_id
        self.scheduler = scheduler_class()
        self.project = submit_project
        self.submit_queue = submit_queue
        self.job_mode = job_mode
        self.filter_tags = filter_tags
        self.num_queued_jobs = num_queued_jobs
        self.num_nodes = num_nodes
        self.wall_time_min = wall_time_min
        self.username = getpass.getuser()

    def get_next_submission(self):
        return {
            "project": self.project,
            "queue": self.submit_queue,
            "job_mode": self.job_mode,
            "filter_tags": self.filter_tags,
            "num_nodes": self.num_nodes,
            "wall_time_min": self.wall_time_min,
        }

    def run_cycle(self):
        scheduler_jobs = self.scheduler.get_statuses(
            user=self.username, queue=self.submit_queue
        )
        if len(scheduler_jobs) < self.num_queued_jobs:
            sub = self.get_next_submission()
            new_job = BatchJob(**sub)
            new_job.save()
            logger.info(f"Submitted new BatchJob: {new_job}")

    def cleanup(self):
        logger.info(f"Exiting QueueMaintainer service")
