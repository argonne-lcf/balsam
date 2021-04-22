import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional, Set, Type

import zmq

from balsam.config import SiteConfig
from balsam.schemas import JobState
from balsam.site import ApplicationDefinition, BulkStatusUpdater, FixedDepthJobSource
from balsam.site.launcher.util import countdown_timer_min
from balsam.util import SigHandler

if TYPE_CHECKING:
    from balsam._api.models import Job
    from balsam.platform.app_run import AppRun  # noqa: F401

logger = logging.getLogger(__name__)


class Master:
    def __init__(
        self,
        job_source: FixedDepthJobSource,
        status_updater: BulkStatusUpdater,
        app_cache: Dict[int, Type[ApplicationDefinition]],
        wall_time_min: int,
        master_port: int,
        data_dir: Path,
        idle_ttl_sec: int,
        num_workers: int,
    ) -> None:
        self.job_source = job_source
        self.status_updater = status_updater
        self.app_cache = app_cache
        self.data_dir = data_dir
        self.remaining_timer = countdown_timer_min(wall_time_min, delay_sec=0)
        self.idle_ttl_sec = idle_ttl_sec
        self.idle_time: Optional[float] = None
        self.active_ids: Set[int] = set()
        self.occupancies: Dict[int, float] = {}
        self.num_workers = num_workers
        self.master_port = master_port

        next(self.remaining_timer)

        self.sig_handler = SigHandler()
        self.status_updater.start()
        self.job_source.start()
        logger.debug("Job source/status updater created")

    def job_to_dict(self, job: "Job") -> Dict[str, Any]:
        app_cls = self.app_cache[job.app_id]
        app = app_cls(job)
        workdir = self.data_dir.joinpath(app.job.workdir).as_posix()

        preamble = app.shell_preamble()
        app_command = app.get_arg_str()
        environ_vars = app.get_environ_vars()
        occ = 1.0 / job.node_packing_count
        assert job.id is not None
        self.occupancies[job.id] = occ
        return dict(
            id=job.id,
            cwd=workdir,
            cmdline=app_command,
            preamble=preamble,
            node_occupancy=occ,
            envs=environ_vars,
            threads_per_rank=job.threads_per_rank,
            threads_per_core=job.threads_per_core,
            gpus_per_rank=job.gpus_per_rank,
        )

    def handle_request(self) -> None:
        msg = self.socket.recv_json()
        now = datetime.utcnow()
        finished_ids = set()
        for id in msg["done"]:
            self.status_updater.put(
                id,
                JobState.run_done,
                state_timestamp=now,
            )
            finished_ids.add(id)
        for (id, retcode, tail) in msg["error"]:
            self.status_updater.put(
                id,
                JobState.run_error,
                state_timestamp=now,
                state_data={
                    "returncode": retcode,
                    "error": tail,
                },
            )
            finished_ids.add(id)
        for id in msg["started"]:
            self.status_updater.put(
                id,
                JobState.running,
                state_timestamp=now,
                state_data={"num_nodes": self.occupancies.pop(id, 1.0)},
            )
            self.active_ids.add(id)

        self.active_ids -= finished_ids

        src = msg["source"]
        max_jobs = msg["request_num_jobs"]
        logger.debug(f"Worker {src} requested {max_jobs} jobs")

        next_jobs = self.job_source.get_jobs(max_jobs)
        new_job_specs = [self.job_to_dict(job) for job in next_jobs]
        self.socket.send_json({"new_jobs": new_job_specs})
        if new_job_specs:
            logger.debug(f"Sent {len(new_job_specs)} new jobs to {src}")

    def idle_check(self) -> None:
        if not self.active_ids:
            if self.idle_time is None:
                self.idle_time = time.time()
            if time.time() - self.idle_time > self.idle_ttl_sec:
                logger.info(f"Nothing to do for {self.idle_ttl_sec} seconds: quitting")
                self.sig_handler.set()
        else:
            self.idle_time = None

    def run(self) -> None:
        logger.debug("In master run")
        try:
            self.context = zmq.Context()  # type: ignore
            self.context.setsockopt(zmq.LINGER, 0)  # type: ignore
            self.socket = self.context.socket(zmq.REP)  # type: ignore
            self.socket.bind(f"tcp://*:{self.master_port}")
            logger.debug("Master ZMQ socket bound.")

            for remaining_minutes in self.remaining_timer:
                logger.debug(f"{remaining_minutes} minutes remaining")
                self.handle_request()
                self.idle_check()
                if self.sig_handler.is_set():
                    logger.info("Signal: master breaking main loop")
                    break
        except:  # noqa
            raise
        finally:
            self.shutdown()
            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.close(linger=0)
            self.context.term()  # type: ignore
            logger.info("shutdown done: ensemble master exit gracefully")

    def shutdown(self) -> None:
        now = datetime.utcnow()
        logger.info(f"Timing out {len(self.active_ids)} active runs")
        for id in self.active_ids:
            self.status_updater.put(
                id,
                JobState.run_timeout,
                state_timestamp=now,
            )

        logger.info("Terminating StatusUpdater...")
        # StatusUpdater needs to join first: stop the RUNNING updates
        self.status_updater.terminate()
        self.status_updater.join()
        logger.info("StatusUpdater has joined.")

        # We trigger JobSourceexit after StatusUpdater has joined to ensure
        # *all* Jobs get properly released
        logger.info("Terminating JobSource...")
        self.job_source.terminate()
        self.job_source.join()
        logger.info("JobSource has joined.")
        logger.info("Master sending exit message to all Workers")
        for _ in range(self.num_workers):
            self.socket.recv_json()
            self.socket.send_json({"exit": True})
        logger.info("All workers have received exit message. Quitting.")


def master_main(wall_time_min: int, master_port: int, log_filename: str, num_workers: int, filter_tags: str) -> None:
    site_config = SiteConfig()
    filter_tags_dict: Optional[Dict[str, str]] = json.loads(filter_tags)

    site_config.enable_logging("serial_mode", filename=log_filename + ".master")
    logger.debug("Launching master")

    App = site_config.client.App
    app_cache = {
        app.id: ApplicationDefinition.load_app_class(site_config.apps_path, app.class_path)
        for app in App.objects.filter(site_id=site_config.site_id)
        if app.id is not None
    }

    launch_settings = site_config.settings.launcher
    node_cls = launch_settings.compute_node
    scheduler_id = node_cls.get_scheduler_id()
    job_source = FixedDepthJobSource(
        client=site_config.client,
        site_id=site_config.site_id,
        prefetch_depth=num_workers * launch_settings.serial_mode_prefetch_per_rank,
        filter_tags=filter_tags_dict,
        max_wall_time_min=wall_time_min,
        scheduler_id=scheduler_id,
        serial_only=True,
        max_nodes_per_job=1,
        app_ids={app_id for app_id in app_cache if app_id is not None},
    )
    status_updater = BulkStatusUpdater(site_config.client)

    master = Master(
        job_source=job_source,
        status_updater=status_updater,
        app_cache=app_cache,
        wall_time_min=wall_time_min,
        master_port=int(master_port),
        data_dir=site_config.data_path,
        idle_ttl_sec=launch_settings.idle_ttl_sec,
        num_workers=num_workers,
    )
    master.run()
