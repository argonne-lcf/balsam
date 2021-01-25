import click
from datetime import datetime
import multiprocessing
import json
import sys
import logging
import signal
import time
import socket
import subprocess
import zmq

from balsam.config import SiteConfig
from balsam.site import FixedDepthJobSource, BulkStatusUpdater, ApplicationDefinition
from balsam.platform import TimeoutExpired
from balsam.site.launcher.node_manager import NodeManager, InsufficientResources
from balsam.site.launcher.util import countdown_timer_min

logger = logging.getLogger("balsam.site.launcher.serial_mode")
EXIT_FLAG = False


def handle_term(signum, stack):
    global EXIT_FLAG
    EXIT_FLAG = True


class Master:
    def __init__(
        self,
        job_source,
        status_updater,
        app_cache,
        wall_time_min,
        master_port,
        data_dir,
        idle_ttl_sec,
        num_workers,
    ):
        self.job_source = job_source
        self.status_updater = status_updater
        self.app_cache = app_cache
        self.data_dir = data_dir
        self.remaining_timer = countdown_timer_min(wall_time_min, delay_sec=0.0)
        self.idle_ttl_sec = idle_ttl_sec
        self.idle_time = None
        self.active_ids = set()
        self.num_workers = num_workers

        next(self.remaining_timer)

        self.status_updater.start()
        self.job_source.start()
        logger.debug("Job source/status updater created")

        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(f"tcp://*:{master_port}")
        logger.debug("Master ZMQ socket bound.")

    def job_to_dict(self, job):
        app_cls = self.app_cache[job.app_id]
        app = app_cls(job)
        workdir = self.data_dir.joinpath(app.job.workdir)

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

    def handle_request(self):
        msg = self.socket.recv_json()
        now = datetime.utcnow()
        finished_ids = set()
        for id in msg["done"]:
            self.status_updater.put(
                id,
                "RUN_DONE",
                state_timestamp=now,
            )
            finished_ids.add(id)
        for (id, retcode, tail) in msg["error"]:
            self.status_updater.put(
                id,
                "RUN_ERROR",
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
                "RUNNING",
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

    def idle_check(self):
        global EXIT_FLAG
        if not self.active_ids:
            if self.idle_time is None:
                self.idle_time = time.time()
            if time.time() - self.idle_time > self.idle_ttl_sec:
                logger.info(f"Nothing to do for {self.idle_ttl_sec} seconds: quitting")
                EXIT_FLAG = True
        else:
            self.idle_time = None

    def run(self):
        global EXIT_FLAG
        logger.debug("In master run")
        for remaining_minutes in self.remaining_timer:
            logger.debug(f"{remaining_minutes} minutes remaining")
            self.handle_request()
            self.idle_check()
            if EXIT_FLAG:
                logger.info("EXIT_FLAG on; master breaking main loop")
                break

        self.shutdown()
        logger.info("shutdown done: ensemble master exit gracefully")

    def shutdown(self):
        logger.info("Master sending exit message to all Workers")
        for _ in range(self.num_workers):
            self.socket.recv_json()
            self.socket.send_json({"exit": True})

        now = datetime.utcnow()
        logger.info(f"Timing out {len(self.active_ids)} active runs")
        for id in self.active_ids:
            self.status_updater.put(
                id,
                "RUN_TIMEOUT",
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


class Worker:
    CHECK_PERIOD = 0.1
    RETRY_WINDOW = 20
    RETRY_CODES = [-11, 1, 255, 12345]
    MAX_RETRY = 3

    def __init__(
        self,
        app_run,
        node_manager,
        master_host,
        master_port,
        delay_sec,
        error_tail_num_lines,
        num_prefetch_jobs,
    ):
        self.hostname = socket.gethostname()
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.app_run = app_run
        self.node_manager = node_manager
        self.master_address = f"tcp://{master_host}:{master_port}"
        self.delay_sec = delay_sec
        self.error_tail_num_lines = error_tail_num_lines
        self.num_prefetch_jobs = num_prefetch_jobs

        self.app_runs = {}
        self.start_times = {}
        self.retry_counts = {}
        self.job_specs = {}
        self.node_specs = {}
        self.runnable_cache = {}

    def cleanup_proc(self, id, timeout=0):
        self.kill(id, timeout=timeout)
        self.node_manager.free(id)
        for d in (
            self.app_runs,
            self.start_times,
            self.retry_counts,
            self.job_specs,
            self.node_specs,
        ):
            del d[id]

    def check_retcodes(self):
        id_retcodes = []
        for id, proc in self.app_runs.items():
            retcode = proc.poll()
            id_retcodes.append((id, retcode))
        return id_retcodes

    def log_error_tail(self, id, retcode):
        tail = self.app_runs[id].tail_output(self.error_tail_num_lines)
        logmsg = f"Job {id} nonzero return {retcode}:\n {tail}"
        logger.error(logmsg)
        return tail

    def can_retry(self, id, retcode):
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

    def kill(self, id, timeout=0):
        p = self.app_runs[id]
        if p.poll() is None:
            p.terminate()
            logger.debug(
                f"worker {self.hostname} sent TERM to {id}...waiting on shutdown"
            )
            try:
                p.wait(timeout=timeout)
            except TimeoutExpired:
                p.kill()

    def launch_run(self, id):
        job_spec = self.job_specs[id].copy()
        node_spec = self.node_specs[id]
        job_spec.pop("id")
        job_spec.pop("node_occupancy")

        logger.debug(f"Job {id} WORKER_START")
        proc = self.app_run(
            **job_spec,
            **node_spec,
            ranks_per_node=1,
            launch_params={},
            outfile_path=job_spec["cwd"].joinpath("job.out"),
        )
        self.app_runs[id] = proc

    def handle_error(self, id, retcode):
        tail = self.log_error_tail(id, retcode)

        if not self.can_retry(id, retcode):
            self.cleanup_proc(id)
            return (retcode, tail)
        else:
            self.start_times[id] = time.time()
            self.retry_counts[id] += 1
            self.launch_run(id)
            return "running"

    def poll_processes(self):
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
                    retcode, tail = status
                    error.append((id, retcode, tail))
        return done, error

    def exit(self):
        ids = list(self.app_runs.keys())
        for id in ids:
            self.cleanup_proc(id, timeout=self.CHECK_PERIOD)
        sys.exit(0)

    def start_jobs(self):
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

        self.runnable_cache = {
            k: v for k, v in self.runnable_cache.items() if k not in started_ids
        }
        return started_ids

    def run(self):
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
                self.runnable_cache.update(
                    {job["id"]: job for job in response_msg["new_jobs"]}
                )

            logger.debug(
                f"{self.hostname} fraction available: {self.node_manager.aggregate_free_nodes()} "
                f"[{len(self.runnable_cache)} additional prefetched "
                f"jobs in cache]"
            )

            if EXIT_FLAG:
                logger.info(f"Worker {self.hostname} EXIT_FLAG break")
                break

        self.exit()


def launch_master_subprocess():
    args = [sys.executable] + sys.argv + ["--run-master"]
    return subprocess.Popen(args)


def run_master_launcher(
    site_config, wall_time_min, master_port, num_workers, filter_tags
):
    node_cls = site_config.launcher.compute_node
    batch_job_id = node_cls.get_batch_job_id()
    job_source = FixedDepthJobSource(
        client=site_config.client,
        site_id=site_config.site_id,
        prefetch_depth=num_workers * site_config.launcher.serial_mode_prefetch_per_rank,
        filter_tags=filter_tags,
        max_wall_time_min=wall_time_min,
        batch_job_id=batch_job_id,
        serial_only=True,
        max_nodes_per_job=1,
    )
    status_updater = BulkStatusUpdater(site_config.client)

    App = site_config.client.App
    app_cache = {
        app.id: ApplicationDefinition.load_app_class(
            site_config.apps_path, app.class_path
        )
        for app in App.objects.filter(site_id=site_config.site_id)
    }

    master = Master(
        job_source=job_source,
        status_updater=status_updater,
        app_cache=app_cache,
        wall_time_min=wall_time_min,
        master_port=master_port,
        data_dir=site_config.data_path,
        idle_ttl_sec=site_config.launcher.idle_ttl_sec,
        num_workers=num_workers,
    )
    master.run()


def run_worker(site_config, master_host, master_port, hostname):
    node_cls = site_config.launcher.compute_node
    nodes = [node for node in node_cls.get_job_nodelist() if node.hostname == hostname]
    node_manager = NodeManager(
        nodes, allow_node_packing=site_config.launcher.mpirun_allows_node_packing
    )
    worker = Worker(
        app_run=site_config.launcher.local_app_launcher,
        node_manager=node_manager,
        master_host=master_host,
        master_port=master_port,
        delay_sec=site_config.launcher.delay_sec,
        error_tail_num_lines=site_config.launcher.error_tail_num_lines,
        num_prefetch_jobs=site_config.launcher.serial_mode_prefetch_per_rank,
    )
    worker.run()


@click.command()
@click.option("--wall-time-min", type=int)
@click.option("--master-address")
@click.option("--run-master", is_flag=True, default=False)
@click.option("--log-filename")
@click.option("--num-workers")
@click.option("--filter-tags")
def main(
    wall_time_min, master_address, run_master, log_filename, num_workers, filter_tags
):
    master_host, master_port = master_address.split(":")
    site_config = SiteConfig()
    filter_tags = json.loads(filter_tags)
    hostname = socket.gethostname()
    master_proc = None

    if run_master:
        site_config.enable_logging("serial_mode", filename=log_filename + ".master")
        run_master_launcher(
            site_config,
            wall_time_min,
            master_port,
            num_workers,
            filter_tags,
        )
    else:
        site_config.enable_logging(
            "serial_mode", filename=log_filename + f".{hostname}"
        )
        if hostname == master_host:
            master_proc = launch_master_subprocess()
        run_worker(site_config, master_host, master_port, hostname)
        if master_proc is not None:
            master_proc.terminate()
            master_proc.wait()


if __name__ == "__main__":
    multiprocessing.set_start_method("fork", force=True)
    signal.signal(signal.SIGINT, handle_term)
    signal.signal(signal.SIGTERM, handle_term)
    main()
