from .service_base import BalsamService
from balsam.site import ProcessingJobSource, StatusUpdater
from balsam.site import ApplicationDefinition
from balsam.api.models import App
import multiprocessing
import signal
import queue
import logging

logger = logging.getLogger(__name__)


class ProcessingWorker(multiprocessing.Process):
    TRANSITIONS = {
        "STAGED_IN": lambda app: app.preprocess(),
        "RUN_DONE": lambda app: app.postprocess(),
        "RUN_ERROR": lambda app: app.handle_error(),
        "RUN_TIMEOUT": lambda app: app.handle_timeout(),
    }

    def run(self, job_source, status_updater, app_cache):
        EXIT_FLAG = False

        def handler(signum, stack):
            nonlocal EXIT_FLAG
            EXIT_FLAG = True

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)
        while not EXIT_FLAG:
            try:
                job = job_source.get(timeout=1)
            except queue.Empty:
                continue
            else:
                handler = self.TRANSITIONS[job.state]
                app = app_cache[job.app_id](job)
                handler(app)
                status_updater.put_nowait(job)
                logger.debug(f"{job.id} advanced to {job.state}")


class ProcessingService(BalsamService):
    def __init__(
        self,
        client,
        site_id,
        prefetch_depth,
        apps_path,
        service_period=60,
        filter_tags=None,
        num_workers=5,
    ):
        super().__init__(service_period=service_period)
        self.site_id = site_id
        self.load_site_apps(apps_path)
        self.job_source = ProcessingJobSource(
            client, site_id, prefetch_depth, filter_tags,
        )
        self.status_updater = StatusUpdater(client, site_id)
        self.workers = [
            ProcessingWorker(
                args=(self.job_source.queue, self.status_updater.queue, self.app_cache)
            )
            for _ in range(num_workers)
        ]
        self._started = False

    def load_site_apps(self, apps_path):
        self.app_cache = {}
        apps = App.objects.filter(site_id=self.site_id)
        for app in apps:
            self.app_cache[app.id] = ApplicationDefinition.load_app_class(
                apps_path, app.class_path
            )

    def run_cycle(self):
        if not self._started:
            self.status_updater.start()
            self.job_source.start()
            for worker in self.workers:
                worker.start()
            self._started = True

    def cleanup(self):
        self.job_source.set_exit()
        for worker in self.workers:
            worker.terminate()
        for worker in self.workers:
            worker.join()
        self.job_source.join()
        self.status_updater.set_exit()
        self.status_updater.join()
