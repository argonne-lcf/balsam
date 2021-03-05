import getpass
import logging
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Type

from balsam.schemas import RUNNABLE_STATES, JobState, SchedulerBackfillWindow, SchedulerJobStatus

from .service_base import BalsamService

if TYPE_CHECKING:
    from balsam.client import RESTClient
    from balsam.platform.scheduler import SchedulerInterface  # noqa: F401

logger = logging.getLogger(__name__)


class ElasticQueueService(BalsamService):
    def __init__(
        self,
        client: "RESTClient",
        site_id: int,
        scheduler_class: Type["SchedulerInterface"],
        service_period: int = 60,
        submit_project: str = "datascience",
        submit_queue: str = "balsam",
        job_mode: str = "mpi",
        filter_tags: Optional[Dict[str, str]] = None,
        min_wall_time_min: int = 35,
        wall_time_pad_min: int = 5,
        min_num_nodes: int = 20,
        max_num_nodes: int = 127,
        max_queue_wait_time_min: int = 10,
        max_queued_jobs: int = 20,
    ) -> None:
        super().__init__(client=client, service_period=service_period)
        if wall_time_pad_min >= min_wall_time_min:
            raise ValueError("Pad walltime must be less than minimum batch job walltime.")
        self.site_id = site_id
        self.scheduler = scheduler_class()
        self.project = submit_project
        self.submit_queue = submit_queue
        self.job_mode = job_mode
        self.filter_tags = filter_tags
        self.max_queue_wait_time_min = max_queue_wait_time_min
        self.min_wall_time_min = min_wall_time_min
        self.wall_time_pad_min = wall_time_pad_min
        self.min_num_nodes = min_num_nodes
        self.max_num_nodes = max_num_nodes
        self.max_queued_jobs = max_queued_jobs
        self.username = getpass.getuser()

    def _get_largest_backfill_window(self, min_num_nodes: int) -> Optional[SchedulerBackfillWindow]:
        windows_by_queue: Dict[str, List[SchedulerBackfillWindow]] = self.scheduler.get_backfill_windows()
        windows = windows_by_queue.get(self.submit_queue, [])
        windows = [w for w in windows if w.wall_time_min >= self.min_wall_time_min and w.num_nodes >= min_num_nodes]
        windows = sorted(windows, key=lambda w: w.wall_time_min * w.num_nodes, reverse=True)
        return windows[0] if windows else None

    def get_next_submission(self, scheduler_jobs: Iterable[SchedulerJobStatus]) -> Optional[Dict[str, Any]]:
        Job = self.client.Job
        queued_batchjobs = [j for j in scheduler_jobs if j.state in ("queued", "pending_submission")]
        running_batchjobs = [j for j in scheduler_jobs if j.state == "running"]
        num_reserved_nodes = sum(j.num_nodes for j in queued_batchjobs) + sum(j.num_nodes for j in running_batchjobs)
        logger.debug(
            f"There are {len(queued_batchjobs)} queued, {len(running_batchjobs)} running BatchJobs "
            f"totalling {num_reserved_nodes} nodes."
        )

        running_jobs = Job.objects.filter(site_id=self.site_id, state=JobState.running)
        runnable_jobs = Job.objects.filter(site_id=self.site_id, state=RUNNABLE_STATES)
        running_num_nodes = sum(float(job.num_nodes) / job.node_packing_count for job in running_jobs)
        runnable_num_nodes = sum(float(job.num_nodes) / job.node_packing_count for job in runnable_jobs)
        logger.debug(
            f"{running_num_nodes} nodes are currently occupied; runnable node footprint is {runnable_num_nodes}"
        )

        # The number of nodes currently or soon to be allocated, minus the footprint of currently running jobs
        idle_node_count = num_reserved_nodes - running_num_nodes

        window = self._get_largest_backfill_window(min_num_nodes=min(job.num_nodes for job in runnable_jobs))
        logger.debug(f"Largest backfill window: {window}")

        if len(queued_batchjobs) + len(running_batchjobs) > self.max_queued_jobs:
            logger.info(f"At {self.max_queued_jobs} max queued jobs; will not submit")
            return None

        if not window:
            logger.info("No eligible backfill windows; will not submit")
            return None

        if runnable_num_nodes > idle_node_count:
            request_num_nodes = max(self.min_num_nodes, runnable_num_nodes - idle_node_count)
            request_num_nodes = min(
                request_num_nodes,
                window.num_nodes,
                self.max_num_nodes,
            )
            return {
                "project": self.project,
                "queue": self.submit_queue,
                "job_mode": self.job_mode,
                "filter_tags": self.filter_tags,
                "num_nodes": request_num_nodes,
                "wall_time_min": window.wall_time_min - self.wall_time_pad_min,
            }
        logger.info("Idle node count exceeds runnable footprint; no need to submit.")
        return None

    def run_cycle(self) -> None:
        scheduler_jobs = self.scheduler.get_statuses(user=self.username, queue=self.submit_queue)
        sub = self.get_next_submission(scheduler_jobs.values())
        if sub:
            new_job = self.client.BatchJob(**sub)
            new_job.save()
            logger.info(f"Submitted new BatchJob: {new_job}")

        cancel_jobs = [
            job
            for job in scheduler_jobs.values()
            if job.state == "queued" and job.queued_time_min > self.max_queue_wait_time_min
        ]
        for job in cancel_jobs:
            self.scheduler.delete_job(job.scheduler_id)
            logger.info(f"Deleted queued BatchJob {job.scheduler_id}: exceed max queue wait time")

    def cleanup(self) -> None:
        logger.info("Exiting ElasticQueue service")
