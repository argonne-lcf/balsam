import json
import logging
import signal
import socket
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple, Type, Union, cast

import click
import zmq

from balsam.config import SiteConfig
from balsam.platform import TimeoutExpired
from balsam.schemas import JobState
from balsam.site import ApplicationDefinition, BulkStatusUpdater, FixedDepthJobSource
from balsam.site.launcher.node_manager import InsufficientResources, NodeManager, NodeSpec
from balsam.site.launcher.util import countdown_timer_min

if TYPE_CHECKING:
    from balsam._api.models import Job
    from balsam.platform.app_run import AppRun  # noqa: F401

logger = logging.getLogger("balsam.site.launcher.serial_mode")
EXIT_FLAG = False


def handle_term(signum: int, stack: Any) -> None:
    global EXIT_FLAG
    EXIT_FLAG = True


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
        self.num_workers = num_workers

        next(self.remaining_timer)

        self.status_updater.start()
        self.job_source.start()
        logger.debug("Job source/status updater created")

        self.context = zmq.Context()  # type: ignore
        self.socket = self.context.socket(zmq.REP)  # type: ignore
        self.socket.bind(f"tcp://*:{master_port}")
        logger.debug("Master ZMQ socket bound.")

    def job_to_dict(self, job: "Job") -> Dict[str, Any]:
        app_cls = self.app_cache[job.app_id]
        app = app_cls(job)
        workdir = self.data_dir.joinpath(app.job.workdir).as_posix()

        preamble = app.shell_preamble()
        app_command = app.get_arg_str()
        environ_vars = app.get_environ_vars()
        return dict(
            id=job.id,
            cwd=workdir,
            cmdline=app_command,
            preamble=preamble,
            node_occupancy=1.0 / job.node_packing_count,
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
        global EXIT_FLAG
        if not self.active_ids:
            if self.idle_time is None:
                self.idle_time = time.time()
            if time.time() - self.idle_time > self.idle_ttl_sec:
                logger.info(f"Nothing to do for {self.idle_ttl_sec} seconds: quitting")
                EXIT_FLAG = True
        else:
            self.idle_time = None

    def run(self) -> None:
        global EXIT_FLAG
        logger.debug("In master run")
        try:
            for remaining_minutes in self.remaining_timer:
                logger.debug(f"{remaining_minutes} minutes remaining")
                self.handle_request()
                self.idle_check()
                if EXIT_FLAG:
                    logger.info("EXIT_FLAG on; master breaking main loop")
                    break
        except:  # noqa
            raise
        finally:
            self.shutdown()
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

        # We trigger JobSource exit after StatusUpdater has joined to ensure
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


class Worker:
    CHECK_PERIOD = 0.1
    RETRY_WINDOW = 20
    RETRY_CODES = [-11, 1, 255, 12345]
    MAX_RETRY = 3

    def __init__(
        self,
        app_run: Type["AppRun"],
        node_manager: NodeManager,
        master_host: str,
        master_port: int,
        delay_sec: int,
        error_tail_num_lines: int,
        num_prefetch_jobs: int,
    ) -> None:
        self.hostname = socket.gethostname()
        self.context = zmq.Context()  # type: ignore
        self.socket = self.context.socket(zmq.REQ)  # type: ignore
        self.app_run = app_run
        self.node_manager = node_manager
        self.master_address = f"tcp://{master_host}:{master_port}"
        self.delayer = countdown_timer_min(4320, delay_sec=delay_sec)
        self.error_tail_num_lines = error_tail_num_lines
        self.num_prefetch_jobs = num_prefetch_jobs

        self.app_runs: Dict[int, "AppRun"] = {}
        self.start_times: Dict[int, float] = {}
        self.retry_counts: Dict[int, int] = {}
        self.job_specs: Dict[int, Dict[str, Any]] = {}
        self.node_specs: Dict[int, NodeSpec] = {}
        self.runnable_cache: Dict[int, Dict[str, Any]] = {}

    def cleanup_proc(self, id: int, timeout: float = 0) -> None:
        self.kill(id, timeout=timeout)
        self.node_manager.free(id)
        del self.app_runs[id]
        del self.start_times[id]
        del self.retry_counts[id]
        del self.job_specs[id]
        del self.node_specs[id]

    def check_retcodes(self) -> List[Tuple[int, Optional[int]]]:
        id_retcodes = []
        for id, proc in self.app_runs.items():
            retcode = proc.poll()
            id_retcodes.append((id, retcode))
        return id_retcodes

    def log_error_tail(self, id: int, retcode: int) -> str:
        tail = self.app_runs[id].tail_output(self.error_tail_num_lines)
        logmsg = f"Job {id} nonzero return {retcode}:\n {tail}"
        logger.error(logmsg)
        return tail

    def can_retry(self, id: int, retcode: int) -> bool:
        if retcode in self.RETRY_CODES:
            elapsed = time.time() - self.start_times[id]
            retry_count = self.retry_counts[id]
            if elapsed < self.RETRY_WINDOW and retry_count <= self.MAX_RETRY:
                logmsg = (
                    f"Job {id} can retry task (err occured after {elapsed:.2f} sec; "
                    f"attempt {self.retry_counts[id]}/{self.MAX_RETRY})"
                )
                logger.error(logmsg)
                return True
        return False

    def kill(self, id: int, timeout: float = 0) -> None:
        p = self.app_runs[id]
        if p.poll() is None:
            p.terminate()
            logger.debug(f"worker {self.hostname} sent TERM to {id}...waiting on shutdown")
            try:
                p.wait(timeout=timeout)
            except TimeoutExpired:
                p.kill()

    def launch_run(self, id: int) -> None:
        job_spec = self.job_specs[id].copy()
        node_spec = self.node_specs[id]
        job_spec.pop("id")
        job_spec.pop("node_occupancy")

        logger.debug(f"Job {id} WORKER_START")
        proc = self.app_run(
            **job_spec,
            node_spec=node_spec,
            ranks_per_node=1,
            launch_params={},
            outfile_path=Path(job_spec["cwd"]).joinpath("job.out"),
        )
        proc.start()
        self.app_runs[id] = proc

    def handle_error(self, id: int, retcode: int) -> Union[Tuple[int, str], str]:
        tail = self.log_error_tail(id, retcode)

        if not self.can_retry(id, retcode):
            self.cleanup_proc(id)
            return (retcode, tail)
        else:
            self.start_times[id] = time.time()
            self.retry_counts[id] += 1
            self.launch_run(id)
            return "running"

    def poll_processes(self) -> Tuple[List[int], List[Tuple[int, int, str]]]:
        done, error = [], []
        for id, retcode in self.check_retcodes():
            if retcode is None:
                continue
            elif retcode == 0:
                done.append(id)
                logger.debug(f"Job {id} WORKER_DONE")
                self.cleanup_proc(id)
            else:
                logger.info(f"Job {id} WORKER_ERROR")
                status = self.handle_error(id, retcode)
                if status != "running":
                    retcode, tail = cast(Tuple[int, str], status)
                    error.append((id, retcode, tail))
        return done, error

    def exit(self) -> None:
        ids = list(self.app_runs.keys())
        for id in ids:
            self.cleanup_proc(id, timeout=self.CHECK_PERIOD)
        sys.exit(0)

    def start_jobs(self) -> List[int]:
        started_ids = []

        for id, job_spec in self.runnable_cache.items():
            try:
                node_spec = self.node_manager.assign_from_params(
                    **job_spec,
                    num_nodes=1,
                    ranks_per_node=1,
                )
            except InsufficientResources:
                continue
            else:
                self.job_specs[id] = job_spec
                self.node_specs[id] = node_spec
                self.start_times[id] = time.time()
                self.retry_counts[id] = 1
                self.launch_run(id)
                started_ids.append(id)

        self.runnable_cache = {k: v for k, v in self.runnable_cache.items() if k not in started_ids}
        return started_ids

    def run(self) -> None:
        global EXIT_FLAG
        self.socket.connect(self.master_address)
        logger.debug(f"Worker connected to {self.master_address}")
        while True:
            done_ids, errors = self.poll_processes()
            started_ids = self.start_jobs()
            request_num_jobs = max(0, self.num_prefetch_jobs - len(self.runnable_cache))

            msg = {
                "source": self.hostname,
                "started": started_ids,
                "done": done_ids,
                "error": errors,
                "request_num_jobs": request_num_jobs,
            }
            self.socket.send_json(msg)
            logger.debug("Worker awaiting response...")
            response_msg = self.socket.recv_json()
            logger.debug("Worker response received")

            if response_msg.get("exit"):
                logger.info(f"Worker {self.hostname} received exit message: break")
                break

            if response_msg.get("new_jobs"):
                self.runnable_cache.update({job["id"]: job for job in response_msg["new_jobs"]})

            logger.debug(
                f"{self.hostname} fraction available: {self.node_manager.aggregate_free_nodes()} "
                f"[{len(self.runnable_cache)} additional prefetched "
                f"jobs in cache]"
            )

            if EXIT_FLAG:
                logger.info(f"Worker {self.hostname} EXIT_FLAG break")
                break
            next(self.delayer)

        self.exit()


def launch_master_subprocess() -> "subprocess.Popen[bytes]":
    args = [sys.executable] + sys.argv + ["--run-master"]
    return subprocess.Popen(args)


def run_master_launcher(
    site_config: SiteConfig,
    wall_time_min: int,
    master_port: int,
    num_workers: int,
    filter_tags: Optional[Dict[str, str]],
) -> None:
    App = site_config.client.App
    app_cache = {
        app.id: ApplicationDefinition.load_app_class(site_config.apps_path, app.class_path)
        for app in App.objects.filter(site_id=site_config.settings.site_id)
        if app.id is not None
    }

    launch_settings = site_config.settings.launcher
    node_cls = launch_settings.compute_node
    scheduler_id = node_cls.get_scheduler_id()
    job_source = FixedDepthJobSource(
        client=site_config.client,
        site_id=site_config.settings.site_id,
        prefetch_depth=num_workers * launch_settings.serial_mode_prefetch_per_rank,
        filter_tags=filter_tags,
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
        master_port=master_port,
        data_dir=site_config.data_path,
        idle_ttl_sec=launch_settings.idle_ttl_sec,
        num_workers=num_workers,
    )
    master.run()


def run_worker(site_config: SiteConfig, master_host: str, master_port: int, hostname: str) -> None:
    launch_settings = site_config.settings.launcher
    node_cls = launch_settings.compute_node
    nodes = [node for node in node_cls.get_job_nodelist() if node.hostname == hostname]
    node_manager = NodeManager(nodes, allow_node_packing=launch_settings.mpirun_allows_node_packing)
    worker = Worker(
        app_run=launch_settings.local_app_launcher,
        node_manager=node_manager,
        master_host=master_host,
        master_port=master_port,
        delay_sec=launch_settings.delay_sec,
        error_tail_num_lines=launch_settings.error_tail_num_lines,
        num_prefetch_jobs=launch_settings.serial_mode_prefetch_per_rank,
    )
    worker.run()


@click.command()
@click.option("--wall-time-min", type=int)
@click.option("--master-address")
@click.option("--run-master", is_flag=True, default=False)
@click.option("--log-filename")
@click.option("--num-workers", type=int)
@click.option("--filter-tags")
def main(
    wall_time_min: int, master_address: str, run_master: bool, log_filename: str, num_workers: int, filter_tags: str
) -> None:
    master_host, master_port = master_address.split(":")
    site_config = SiteConfig()
    filter_tags_dict: Optional[Dict[str, str]] = json.loads(filter_tags)
    hostname = socket.gethostname()
    master_proc = None

    signal.signal(signal.SIGINT, handle_term)
    signal.signal(signal.SIGTERM, handle_term)

    if run_master:
        site_config.enable_logging("serial_mode", filename=log_filename + ".master")
        logger.debug("Launching master")
        run_master_launcher(
            site_config,
            wall_time_min,
            int(master_port),
            num_workers,
            filter_tags_dict,
        )
    else:
        site_config.enable_logging("serial_mode", filename=log_filename + f".{hostname}")
        if hostname == master_host:
            logger.debug("Launching master subprocess")
            master_proc = launch_master_subprocess()

        logger.debug("Launching worker")
        try:
            run_worker(site_config, master_host, int(master_port), hostname)
        except:  # noqa
            raise
        finally:
            if master_proc is not None:
                logger.debug("Sending SIGTERM to master process")
                master_proc.terminate()
                try:
                    master_proc.wait(timeout=10)
                    logger.debug("master process shutdown OK")
                except subprocess.TimeoutExpired:
                    logger.debug("Killing master process")
                    master_proc.kill()
