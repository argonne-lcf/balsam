import logging
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Type, Union, cast

import zmq

from balsam.config import SiteConfig
from balsam.platform import TimeoutExpired
from balsam.site.launcher.node_manager import InsufficientResources, NodeManager, NodeSpec
from balsam.util import SigHandler

if TYPE_CHECKING:
    from balsam.platform.app_run import AppRun  # noqa: F401

logger = logging.getLogger(__name__)


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
        master_subproc: "Optional[subprocess.Popen[bytes]]",
    ) -> None:
        self.sig_handler = SigHandler()
        self.hostname = socket.gethostname()
        self.app_run = app_run
        self.node_manager = node_manager
        self.master_address = f"tcp://{master_host}:{master_port}"
        self.delay_sec = delay_sec
        self.error_tail_num_lines = error_tail_num_lines
        self.num_prefetch_jobs = num_prefetch_jobs
        self.master_subproc = master_subproc

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
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.close(linger=0)
        self.context.term()  # type: ignore

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

    def cycle(self) -> bool:
        """Run a cycle of Job dispatch. Returns True if worker should continue; False if time to exit."""
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
            return False

        if response_msg.get("new_jobs"):
            self.runnable_cache.update({job["id"]: job for job in response_msg["new_jobs"]})

        logger.debug(
            f"{self.hostname} fraction available: {self.node_manager.aggregate_free_nodes()} "
            f"[{len(self.runnable_cache)} additional prefetched "
            f"jobs in cache]"
        )
        return True

    def run(self) -> None:
        self.context = zmq.Context()  # type: ignore
        self.context.setsockopt(zmq.LINGER, 0)  # type: ignore
        self.socket = self.context.socket(zmq.REQ)  # type: ignore
        self.socket.connect(self.master_address)
        logger.debug(f"Worker connected to {self.master_address}")

        # Run the Worker loop until master sends "exit" message. Does not quit on SIGTERM.
        while self.cycle():
            # If SIGTERM has been received, pass onto master and keep waiting for "exit"
            time.sleep(self.delay_sec)
            if self.sig_handler.is_set() and self.master_subproc is not None:
                self.master_subproc.terminate()
                logger.info("Signal: forwarded SIGTERM to master subprocess.")


def launch_master_subprocess() -> "subprocess.Popen[bytes]":
    args = [sys.executable] + sys.argv + ["--run-master"]
    return subprocess.Popen(args)


def worker_main(
    master_host: str,
    master_port: int,
    log_filename: str,
) -> None:
    site_config = SiteConfig()
    hostname = socket.gethostname()
    master_proc = None

    SigHandler()
    site_config.enable_logging("serial_mode", filename=log_filename + f".{hostname}")
    if hostname == master_host:
        logger.info(f"Launching master subprocess on {hostname}")
        master_proc = launch_master_subprocess()
    else:
        logger.info(f"Worker on {hostname} will connect to remote master on {master_host}")

    launch_settings = site_config.settings.launcher
    node_cls = launch_settings.compute_node
    nodes = [node for node in node_cls.get_job_nodelist() if node.hostname == hostname]
    node_manager = NodeManager(nodes, allow_node_packing=True)
    worker = Worker(
        app_run=launch_settings.local_app_launcher,
        node_manager=node_manager,
        master_host=master_host,
        master_port=master_port,
        delay_sec=launch_settings.delay_sec,
        error_tail_num_lines=launch_settings.error_tail_num_lines,
        num_prefetch_jobs=launch_settings.serial_mode_prefetch_per_rank,
        master_subproc=master_proc,
    )

    try:
        logger.debug("Launching worker")
        # Worker does not quit on SIGTERM; wait until master has sent "exit"
        worker.run()
    except:  # noqa
        raise
    finally:
        worker.exit()
        if master_proc is not None:
            try:
                master_proc.wait(timeout=5)
                logger.info("master process shutdown OK")
            except subprocess.TimeoutExpired:
                logger.warning("Force-killing master process")
                master_proc.kill()
