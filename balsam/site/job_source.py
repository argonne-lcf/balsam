from datetime import timedelta
import logging
import threading
import multiprocessing
import queue
import signal
import time

from balsam.api.models import Session
from balsam.api import Manager

Queue = multiprocessing.Queue
Queue().qsize()  # Can raise NotImplementedError on Mac OS

logger = logging.getLogger(__name__)


class SessionThread:
    """
    Creates and maintains lease on a Session object by periodically pinging API in background thread
    """

    TICK_PERIOD = timedelta(minutes=1)

    def __init__(self, site_id, batch_job_id=None):
        self.session = Session.objects.create(
            site_id=site_id, batch_job_id=batch_job_id
        )
        self._schedule_next_tick()

    def _schedule_next_tick(self):
        self.timer = threading.Timer(
            self.TICK_PERIOD.total_seconds(), self._schedule_next_tick
        )
        self.timer.daemon = True
        self.timer.start()
        self._do_tick()

    def _do_tick(self):
        self.session.tick()


class FixedDepthJobSource(multiprocessing.Process):
    """
    A background process maintains a queue of `prefetch_depth` jobs meeting
    the criteria below. Prefer this JobSource to hide API latency for
    high-throughput, when the number of Jobs to process far exceeds the
    number of available compute resources (e.g. 128 nodes handling 100k jobs).

    WARNING: When the number of Jobs becomes comparable to allocated
    resources, launchers using this JobSource may prefetch too much and
    prevent effective work-sharing (i.e. one launcher hogs all the jobs in
    its queue, leaving the other launchers empty-handed).
    """

    def __init__(
        self,
        client,
        site_id,
        prefetch_depth,
        filter_tags=None,
        states={"PREPROCESSED", "RESTART_READY"},
        serial_only=False,
        max_wall_time_min=None,
        max_nodes_per_job=None,
        max_aggregate_nodes=None,
    ):
        super().__init__()
        self.queue = Queue()
        self.prefetch_depth = prefetch_depth

        Manager.set_client(client)
        self.site_id = site_id
        self.session_thread = None
        self.session = None
        self.filter_tags = {} if filter_tags is None else filter_tags
        self.states = states
        self.serial_only = serial_only
        self.max_wall_time_min = max_wall_time_min
        self.max_nodes_per_job = max_nodes_per_job
        self.max_aggregate_nodes = max_aggregate_nodes
        self.start_time = time.time()

    def get_jobs(self, max_num_jobs):
        fetched = []
        for _ in range(max_num_jobs):
            try:
                fetched.append(self.queue.get_nowait())
            except queue.Empty:
                break
        return fetched

    def run(self):
        EXIT_FLAG = False

        def handler(signum, stack):
            nonlocal EXIT_FLAG
            EXIT_FLAG = True

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

        if self.session_thread is None:
            self.session_thread = SessionThread(site_id=self.site_id)
            self.session = self.session_thread.session

        while not EXIT_FLAG:
            time.sleep(1)
            qsize = self.queue.qsize()
            fetch_count = max(0, self.prefetch_depth - qsize)
            logger.debug(
                f"JobSource queue depth is currently {qsize}. Fetching {fetch_count} more"
            )
            if fetch_count:
                params = self._get_acquire_parameters(fetch_count)
                jobs = self.session.acquire_jobs(**params)
                for job in jobs:
                    self.queue.put_nowait(job)
        logger.info("Signal: JobSource cancelling tick thread and deleting API Session")
        self.session_thread.timer.cancel()
        self.session.delete()
        logger.info("JobSource exit graceful")

    def _get_acquire_parameters(self, num_jobs):
        if self.max_wall_time_min:
            elapsed_min = (time.time() - self.start_time) / 60.0
            request_time = self.max_wall_time_min - elapsed_min
        else:
            request_time = None
        return dict(
            max_num_jobs=num_jobs,
            max_nodes_per_job=self.max_nodes_per_job,
            max_aggregate_nodes=self.max_aggregate_nodes,
            max_wall_time_min=request_time,
            serial_only=self.serial_only,
            filter_tags=self.filter_tags,
            states=self.states,
        )


class SynchronousJobSource(object):
    """
    In this JobSource, `get_jobs` invokes a blocking API call and introduces
    latency. However, it allows greater flexibility in launchers requesting
    *just enough* work for the available resources, and is therefore better
    suited for work-sharing between launchers when the number of tasks does
    not far exceed the available resources. (Example: if you want two 5-node
    launchers to effectively split 10 jobs that take 1 hour each, use this
    JobSource to ensure that no resources go unused!)
    """

    def __init__(
        self,
        client,
        site_id,
        filter_tags=None,
        states={"PREPROCESSED", "RESTART_READY"},
        serial_only=False,
        max_wall_time_min=None,
    ):
        Manager.set_client(client)
        self.site_id = site_id
        self.filter_tags = {} if filter_tags is None else filter_tags
        self.states = states
        self.serial_only = serial_only
        self.max_wall_time_min = max_wall_time_min
        self.start_time = time.time()

        self.session_thread = SessionThread(site_id=self.site_id)
        self.session = self.session_thread.session

    def start(self):
        pass

    def terminate(self):
        logger.info("Signal: JobSource cancelling tick thread and deleting API Session")
        self.session_thread.timer.cancel()
        self.session.delete()
        logger.info("JobSource exit graceful")

    def join(self):
        pass

    def get_jobs(self, max_num_jobs, max_nodes_per_job=None, max_aggregate_nodes=None):
        if self.max_wall_time_min:
            elapsed_min = (time.time() - self.start_time) / 60.0
            request_time = self.max_wall_time_min - elapsed_min
        else:
            request_time = None
        jobs = self.session.acquire_jobs(
            max_num_jobs=max_num_jobs,
            max_nodes_per_job=max_nodes_per_job,
            max_aggregate_nodes=max_aggregate_nodes,
            max_wall_time_min=request_time,
            serial_only=self.serial_only,
            filter_tags=self.filter_tags,
            states=self.states,
        )
        return jobs


def get_node_ranges(num_nodes, prefetch_factor, single_node_prefetch_factor):
    """
    Heuristic counts for prefetching jobs of various sizes
    """
    result = []
    num_acquire = prefetch_factor
    while num_nodes:
        lower = min(num_nodes, num_nodes // 2 + 1)
        if num_nodes > 1:
            result.append((lower, num_nodes, num_acquire))
        else:
            result.append((lower, num_nodes, single_node_prefetch_factor))
        num_acquire *= 2
        num_nodes = lower - 1
    return result
