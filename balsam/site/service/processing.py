import logging
import os
import queue
import signal
import sys
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from balsam.site import ApplicationDefinition, BulkStatusUpdater, FixedDepthJobSource
from balsam.util import Process

logger = logging.getLogger(__name__)


@contextmanager
def job_context(workdir: Path, stdout_filename):
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    old_cwd = os.getcwd()
    try:
        os.chdir(workdir)
    except FileNotFoundError:
        workdir.mkdir(parents=True, exist_ok=True)
        os.chdir(workdir)

    try:
        with open(workdir.joinpath(stdout_filename), "a") as fp:
            sys.stdout = fp
            sys.stderr = fp
            yield
    finally:
        os.chdir(old_cwd)
        sys.stdout = old_stdout
        sys.stderr = old_stderr


def transition(app):
    transition_func = {
        "STAGED_IN": app.preprocess,
        "RUN_DONE": app.postprocess,
        "RUN_ERROR": app.handle_error,
        "RUN_TIMEOUT": app.handle_timeout,
    }[app.job.state]
    try:
        msg = f"Running {transition_func.__name__} for Job {app.job.id}"
        logger.debug(msg)
        sys.stdout.write(f"#BALSAM {msg}\n")
        transition_func()
    except Exception as exc:
        logger.exception(f"An exception occured in {transition_func}: marking Job {app.job.id} FAILED")
        app.job.state = "FAILED"
        app.job.state_data = {
            "message": f"An exception occured in {transition_func}",
            "exception": str(exc),
        }


def run_worker(job_source, status_updater, app_cache, data_path):
    EXIT_FLAG = False

    def sig_handler(signum, stack):
        nonlocal EXIT_FLAG
        EXIT_FLAG = True

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)
    data_path = Path(data_path).resolve()

    while not EXIT_FLAG:
        try:
            job = job_source.get(timeout=1)
        except queue.Empty:
            continue
        else:
            app_cls = app_cache[job.app_id]
            app = app_cls(job)
            workdir = data_path.joinpath(app.job.workdir)
            with job_context(workdir, "balsam.log"):
                transition(app)
            job.state_timestamp = datetime.utcnow()
            status_updater.put(
                dict(
                    id=job.id,
                    state=job.state,
                    state_timestamp=datetime.utcnow(),
                    state_data=job.state_data if job.state_data else {},
                )
            )
            logger.debug(f"Job {job.id} advanced to {job.state}")
    logger.info("Signal: ProcessingWorker exit")


class ProcessingService(object):
    def __init__(
        self,
        client,
        site_id,
        prefetch_depth,
        apps_path,
        data_path,
        filter_tags=None,
        num_workers=5,
    ) -> None:
        self.site_id = site_id
        self.job_source = FixedDepthJobSource(
            client=client,
            site_id=site_id,
            prefetch_depth=prefetch_depth,
            filter_tags=filter_tags,
            states={"STAGED_IN", "RUN_DONE", "RUN_ERROR", "RUN_TIMEOUT"},
        )
        self.status_updater = BulkStatusUpdater(client)

        app_cache = {
            app.id: ApplicationDefinition.load_app_class(apps_path, app.class_path)
            for app in client.App.objects.filter(site_id=self.site_id)
        }

        self.workers = [
            Process(
                target=run_worker,
                args=(
                    self.job_source.queue,
                    self.status_updater.queue,
                    app_cache,
                    data_path,
                ),
            )
            for _ in range(num_workers)
        ]
        self._started = False
        logger.info(f"Initialized ProcessingService:\n{self.__dict__}")

    def start(self):
        if not self._started:
            self.status_updater.start()
            self.job_source.start()
            for worker in self.workers:
                worker.start()
            self._started = True

    def terminate(self):
        self.job_source.terminate()
        for worker in self.workers:
            worker.terminate()

    def join(self):
        for worker in self.workers:
            worker.join()
        # Wait til workers DONE before killing status_updater:
        self.status_updater.terminate()
        self.job_source.join()
        self.status_updater.join()
