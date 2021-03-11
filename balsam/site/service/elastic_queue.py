import getpass
import logging
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional

from balsam.schemas import RUNNABLE_STATES, BatchJobState, JobState, SchedulerBackfillWindow

from .service_base import BalsamService

if TYPE_CHECKING:
    from balsam._api.models import BatchJob  # noqa: F401
    from balsam.client import RESTClient

logger = logging.getLogger(__name__)


class ElasticQueueService(BalsamService):
    def __init__(
        self,
        client: "RESTClient",
        site_id: int,
        service_period: int = 60,
        submit_project: str = "datascience",
        submit_queue: str = "balsam",
        job_mode: str = "mpi",
        filter_tags: Optional[Dict[str, str]] = None,
        min_wall_time_min: int = 35,
        max_wall_time_min: int = 360,
        wall_time_pad_min: int = 5,
        min_num_nodes: int = 20,
        max_num_nodes: int = 127,
        max_queue_wait_time_min: int = 10,
        max_queued_jobs: int = 20,
        use_backfill: bool = True,
    ) -> None:
        super().__init__(client=client, service_period=service_period)
        if wall_time_pad_min >= min_wall_time_min:
            raise ValueError("Pad walltime must be less than minimum batch job walltime.")
        self.site_id = site_id
        self.project = submit_project
        self.submit_queue = submit_queue
        self.job_mode = job_mode
        self.filter_tags = filter_tags
        self.max_queue_wait_time_min = max_queue_wait_time_min
        self.min_wall_time_min = min_wall_time_min
        self.max_wall_time_min = max_wall_time_min
        self.wall_time_pad_min = wall_time_pad_min
        self.min_num_nodes = min_num_nodes
        self.max_num_nodes = max_num_nodes
        self.max_queued_jobs = max_queued_jobs
        self.use_backfill = use_backfill
        self.username = getpass.getuser()

    def _get_submission_window(
        self, windows_by_queue: Dict[str, List[SchedulerBackfillWindow]], min_num_nodes: int
    ) -> Optional[SchedulerBackfillWindow]:
        if not self.use_backfill:
            return SchedulerBackfillWindow(num_nodes=self.max_num_nodes, wall_time_min=self.max_wall_time_min)
        windows = windows_by_queue.get(self.submit_queue, [])
        windows = [w for w in windows if w.wall_time_min >= self.min_wall_time_min and w.num_nodes >= min_num_nodes]
        windows = sorted(windows, key=lambda w: w.wall_time_min * w.num_nodes, reverse=True)
        return windows[0] if windows else None

    def get_next_submission(
        self, scheduler_jobs: Iterable["BatchJob"], backfill_windows: Dict[str, List[SchedulerBackfillWindow]]
    ) -> Optional[Dict[str, Any]]:
        Job = self.client.Job
        queued_batchjobs = [j for j in scheduler_jobs if j.state in ("queued", "pending_submission")]
        running_batchjobs = [j for j in scheduler_jobs if j.state == "running"]
        num_reserved_nodes = sum(j.num_nodes for j in queued_batchjobs) + sum(j.num_nodes for j in running_batchjobs)

        logger.debug(
            f"There are {len(queued_batchjobs)} queued, {len(running_batchjobs)} running BatchJobs "
            f"totalling {num_reserved_nodes} nodes."
        )
        if len(queued_batchjobs) + len(running_batchjobs) >= self.max_queued_jobs:
            logger.info(f"At {self.max_queued_jobs} max queued jobs; will not submit")
            return None

        tags = [f"{k}:{v}" for k, v in self.filter_tags.items()] if self.filter_tags else None
        running_jobs = Job.objects.filter(site_id=self.site_id, state=JobState.running, tags=tags)
        runnable_jobs = Job.objects.filter(site_id=self.site_id, state=RUNNABLE_STATES, tags=tags)
        running_num_nodes = sum(float(job.num_nodes) / job.node_packing_count for job in running_jobs)
        runnable_num_nodes = sum(float(job.num_nodes) / job.node_packing_count for job in runnable_jobs)

        logger.debug(
            f"{running_num_nodes} nodes are currently occupied; runnable node footprint is {runnable_num_nodes}"
        )
        if not runnable_jobs:
            logger.info("No Jobs in runnable states; will not submit")
            return None

        window = self._get_submission_window(
            backfill_windows, min_num_nodes=min(job.num_nodes for job in runnable_jobs)
        )
        logger.debug(f"Largest node window: {window}")

        if not window:
            logger.info("No eligible backfill windows; will not submit")
            return None

        # The number of nodes currently or soon to be allocated, minus the footprint of currently running jobs
        idle_node_count = num_reserved_nodes - running_num_nodes

        if runnable_num_nodes > idle_node_count:
            request_num_nodes = max(self.min_num_nodes, runnable_num_nodes - idle_node_count)
            request_num_nodes = min(
                request_num_nodes,
                window.num_nodes,
                self.max_num_nodes,
            )
            return {
                "site_id": self.site_id,
                "project": self.project,
                "queue": self.submit_queue,
                "job_mode": self.job_mode,
                "filter_tags": self.filter_tags,
                "num_nodes": request_num_nodes,
                "wall_time_min": window.wall_time_min - self.wall_time_pad_min,
            }
        logger.info("Idle node count meets or exceeds runnable footprint; no need to submit.")
        return None

    def run_cycle(self) -> None:
        BatchJob = self.client.BatchJob  # noqa: F811
        site = self.client.Site.objects.get(id=self.site_id)
        tags = [f"{k}:{v}" for k, v in self.filter_tags.items()] if self.filter_tags else None
        backfill_windows = site.backfill_windows
        scheduler_jobs = list(
            BatchJob.objects.filter(site_id=self.site_id, filter_tags=tags, queue=self.submit_queue)
        )

        sub = self.get_next_submission(scheduler_jobs, backfill_windows)
        if sub:
            new_job = BatchJob(**sub)
            new_job.save()
            logger.info(f"Submitted new BatchJob: {new_job}")

        cancel_jobs = [
            job
            for job in scheduler_jobs
            if job.state == "queued"
            and job.scheduler_id in site.queued_jobs
            and site.queued_jobs[job.scheduler_id].queued_time_min > self.max_queue_wait_time_min
        ]
        for job in cancel_jobs:
            try:
                batch_job = BatchJob.objects.get(scheduler_id=job.scheduler_id, site_id=self.site_id)
            except BatchJob.DoesNotExist:
                logger.warning(
                    f"Trying to delete BatchJob with scheduler id {job.scheduler_id}, but it does not exist in the API."
                )
            else:
                if batch_job.state != BatchJobState.pending_deletion:
                    batch_job.state = BatchJobState.pending_deletion
                    batch_job.save()
                    logger.info(f"Marked queued BatchJob {job.scheduler_id} for deletion: exceed max queue wait time")

    def cleanup(self) -> None:
        logger.info("Exiting ElasticQueue service")
