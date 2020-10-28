import getpass
from .service_base import BalsamService
from balsam.api import Manager, BatchJob, Job
from balsam.schemas import RUNNABLE_STATES
import logging

logger = logging.getLogger(__name__)


class AutoscaleService(BalsamService):
    def __init__(
        self,
        client,
        site_id,
        scheduler_class,
        service_period=60,
        submit_project="datascience",
        submit_queue="balsam",
        job_mode="mpi",
        filter_tags=None,
        min_wall_time_min=35,
        min_num_nodes=20,
        max_num_nodes=127,
        max_queue_wait_time_min=10,
        max_queued_jobs=20,
    ):
        super().__init__(service_period=service_period)
        Manager.set_client(client)
        self.site_id = site_id
        self.scheduler = scheduler_class()
        self.project = submit_project
        self.submit_queue = submit_queue
        self.job_mode = job_mode
        self.filter_tags = filter_tags
        self.max_queue_wait_time_min = max_queue_wait_time_min
        self.min_wall_time_min = min_wall_time_min
        self.min_num_nodes = min_num_nodes
        self.max_num_nodes = max_num_nodes
        self.max_queued_jobs = max_queued_jobs
        self.username = getpass.getuser()

    def _get_largest_backfill_window(self):
        windows = self.scheduler.get_backfill_windows(queue=self.submit_queue)
        windows = [w for w in windows if w.wall_time_min > self.min_wall_time_min]
        windows = sorted(
            windows, key=lambda w: w.wall_time_min * w.num_nodes, reverse=True
        )
        return windows[0] if windows else None

    def get_next_submission(self, scheduler_jobs):
        queued_jobs = [
            j for j in scheduler_jobs if j.state in ("queued", "pending_submission")
        ]
        running_jobs = [j for j in scheduler_jobs if j.state == "running"]
        num_reserved_nodes = sum(j.num_nodes for j in queued_jobs) + sum(
            j.num_nodes for j in running_jobs
        )
        logger.info(
            f"{len(queued_jobs)} queued; {len(running_jobs)} running; {num_reserved_nodes} nodes reserved in total"
        )

        # Assumption: all jobs num_nodes=1, node_packing_count=1
        num_running = Job.objects.filter(site_id=self.site_id, state="RUNNING").count()
        idle_node_count = num_reserved_nodes - num_running
        runnable = Job.objects.filter(site_id=self.site_id, state=RUNNABLE_STATES)
        required_num_nodes = runnable.count()

        window = self._get_largest_backfill_window()

        if len(queued_jobs) + len(running_jobs) > self.max_queued_jobs:
            logger.info(f"At {self.max_queued_jobs} max queued jobs; will not submit")
            return None

        if window and required_num_nodes > idle_node_count:
            request_num_nodes = max(
                self.min_num_nodes, required_num_nodes - idle_node_count
            )
            request_num_nodes = min(
                request_num_nodes, window.num_nodes, self.max_num_nodes,
            )
            return {
                "project": self.project,
                "queue": self.submit_queue,
                "job_mode": self.job_mode,
                "filter_tags": self.filter_tags,
                "num_nodes": request_num_nodes,
                "wall_time_min": window.wall_time_min,
            }

    def run_cycle(self):
        scheduler_jobs = self.scheduler.get_statuses(
            user=self.username, queue=self.submit_queue
        )
        sub = self.get_next_submission(scheduler_jobs)
        if sub:
            new_job = BatchJob(**sub)
            new_job.save()
            logger.info(f"Submitted new BatchJob: {new_job}")

        cancel_jobs = [
            job
            for job in scheduler_jobs
            if job.state == "queued"
            and job.queued_time_min > self.max_queue_wait_time_min
        ]
        for job in cancel_jobs:
            self.scheduler.delete_job(job.scheduler_id)
            logger.info(
                f"Deleted queued BatchJob {job.scheduler_id}: exceed max queue wait time"
            )

    def cleanup(self):
        logger.info(f"Exiting Autoscaler service")
