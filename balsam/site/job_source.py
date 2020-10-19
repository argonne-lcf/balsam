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


class JobSource(multiprocessing.Process):
    def __init__(self, prefetch_depth):
        super().__init__()
        self._exit_flag = multiprocessing.Event()
        self.queue = Queue()
        self.prefetch_depth = prefetch_depth

    def run(self):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        while not self._exit_flag.is_set():
            time.sleep(1)
            qsize = self.queue.qsize()
            fetch_count = max(0, self.prefetch_depth - qsize)
            logger.debug(
                f"JobSource queue depth is currently {qsize}. Fetching {fetch_count} more"
            )
            if fetch_count:
                jobs = self._acquire_jobs(fetch_count)
                for job in jobs:
                    self.queue.put_nowait(job)
        self._on_exit()

    def get_jobs(self, max_count):
        fetched = []
        for i in range(max_count):
            try:
                fetched.append(self.queue.get_nowait())
            except queue.Empty:
                break
        return fetched

    def get_job(self, block=True, timeout=None):
        return self.queue.get(block=block, timeout=timeout)

    def set_exit(self):
        self._exit_flag.set()

    def _acquire_jobs(self, num_jobs):
        raise NotImplementedError

    def _on_exit(self):
        pass


class SessionThread:
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


class BalsamSessionJobSource(JobSource):
    def __init__(self, client, site_id, prefetch_depth, filter_tags=None):
        super().__init__(prefetch_depth)
        Manager.set_client(client)
        self.site_id = site_id
        self.session_thread = None
        self.session = None
        self.filter_tags = {} if filter_tags is None else filter_tags

    def _acquire_jobs(self, num_jobs):
        if self.session_thread is None:
            self.session_thread = SessionThread(site_id=self.site_id)
            self.session = self.session_thread.session

        acquire_params = self._get_acquire_parameters(num_jobs)
        return self.session.acquire_jobs(**acquire_params)

    def _on_exit(self):
        self.session_thread.timer.cancel()
        self.session.delete()
        logger.info("Stopped Session tick thread and deleted session")


class ProcessingJobSource(BalsamSessionJobSource):
    def _get_acquire_parameters(self, num_jobs):
        return dict(
            max_wall_time_min=50000,
            acquire=[
                {
                    "min_nodes": 1,
                    "max_nodes": 50000,
                    "serial_only": False,
                    "max_num_acquire": num_jobs,
                }
            ],
            filter_tags=self.filter_tags,
            states={"STAGED_IN", "RUN_DONE", "RUN_ERROR", "RUN_TIMEOUT"},
        )
