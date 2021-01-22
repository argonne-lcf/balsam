import click
from datetime import datetime
import logging
import json
import signal
import time

from balsam.config import SiteConfig
from balsam.platform import TimeoutExpired
from balsam.site import SynchronousJobSource, BulkStatusUpdater
from balsam.site import ApplicationDefinition
from .node_manager import NodeManager
from .util import countdown_timer_min

logger = logging.getLogger("balsam.site.launcher.mpi_mode")


class Launcher:
    def __init__(
        self,
        data_dir,
        app_cache,
        idle_ttl_sec,
        app_run,
        node_manager,
        job_source,
        status_updater,
        wall_time_min,
        delay_sec,
        error_tail_num_lines,
        max_concurrent_runs,
    ):
        self.data_dir = data_dir
        self.app_cache = app_cache
        self.idle_ttl_sec = idle_ttl_sec
        self.error_tail_num_lines = error_tail_num_lines
        self.app_run = app_run
        self.node_manager = node_manager
        self.job_source = job_source
        self.status_updater = status_updater
        self.timer = countdown_timer_min(max(1, wall_time_min - 2), delay_sec)
        self.exit_flag = False

        self.status_updater.start()
        self.job_source.start()
        self.active_runs = {}
        self.idle_time = None
        self.max_concurrent_runs = max_concurrent_runs

        def signal_handler(signum, stack):
            self.exit_flag = True

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

    def time_step(self):
        try:
            min_left = next(self.timer)
        except StopIteration:
            self.exit_flag = True
            return

        m, s = map(int, divmod(min_left * 60, 60))
        logger.debug(f"{m:02d}m:{s:02d}s remaining")

    def check_exit(self):
        if not self.active_runs:
            if self.idle_time is None:
                self.idle_time = time.time()
            elif time.time() - self.idle_time > self.idle_ttl_sec:
                self.exit_flag = True
                logger.info(
                    f"Exceeded {self.idle_ttl_sec} sec TTL: shutting down because nothing to do"
                )
        else:
            self.idle_time = None

    def start_job(self, job):
        app_cls = self.app_cache[job.app_id]
        app = app_cls(job)
        workdir = self.data_dir.joinpath(app.job.workdir)

        preamble = app.shell_preamble()
        app_command = app.get_arg_str()
        environ_vars = app.get_environ_vars()

        # assign workers
        node_spec = self.node_manager.assign(job)
        # start run
        run = self.app_run(
            cmdline=app_command,
            preamble=preamble,
            envs=environ_vars,
            cwd=workdir,
            outfile_path=workdir.joinpath("job.out"),
            node_spec=node_spec,
            ranks_per_node=job.ranks_per_node,
            threads_per_rank=job.threads_per_rank,
            threads_per_core=job.threads_per_core,
            launch_params=job.launch_params,
            gpus_per_rank=job.gpus_per_rank,
        )
        return run

    def launch_runs(self):
        max_nodes_per_job = self.node_manager.count_empty_nodes()
        max_aggregate_nodes = self.node_manager.aggregate_free_nodes()
        max_num_to_acquire = max(0, self.max_concurrent_runs - len(self.active_runs))
        if not self.node_manager.allow_node_packing:
            max_num_to_acquire = min(max_num_to_acquire, int(max_aggregate_nodes))

        acquired = self.job_source.get_jobs(
            max_num_jobs=max_num_to_acquire,
            max_nodes_per_job=max_nodes_per_job,
            max_aggregate_nodes=max_aggregate_nodes,
        )
        for job in acquired:
            run = self.start_job(job)
            run.start()
            self.status_updater.put(
                job.id,
                state="RUNNING",
                state_timestamp=datetime.utcnow(),
            )
            self.active_runs[job.id] = run

    def check_run(self, run):
        retcode = run.poll()
        if retcode is None:
            return {"state": "RUNNING"}
        elif retcode == 0:
            return {"state": "RUN_DONE", "state_timestamp": datetime.utcnow()}
        else:
            tail = run.tail_output(nlines=self.error_tail_num_lines)
            return {
                "state": "RUN_ERROR",
                "state_timestamp": datetime.utcnow(),
                "state_data": {"returncode": retcode, "error": tail},
            }

    @staticmethod
    def timeout_kill(runs, timeout=10):
        start = time.time()
        for run in runs:
            remaining = max(0, timeout - (time.time() - start))
            try:
                run.wait(timeout=remaining)
            except TimeoutExpired:
                run.kill()

    def update_states(self, timeout=False):
        remaining_runs = {}
        for id, run in self.active_runs.items():
            status = self.check_run(run)
            if status["state"] == "RUNNING" and timeout:
                run.terminate()
                self.status_updater.put(
                    id, state="RUN_TIMEOUT", state_timestamp=datetime.utcnow()
                )
                self.node_manager.free(id)
                remaining_runs[id] = run
            elif status["state"] == "RUNNING":
                remaining_runs[id] = run
            else:
                self.status_updater.put(id, **status)
                self.node_manager.free(id)
        if timeout:
            self.timeout_kill(remaining_runs.values())
            self.active_runs = {}
        else:
            self.active_runs = remaining_runs

    def run(self):
        try:
            while not self.exit_flag:
                self.time_step()
                self.launch_runs()
                self.update_states()
                self.check_exit()
        except:  # noqa
            raise
        finally:
            logger.info("Launcher starting shutdown sequence")
            self.job_source.terminate()
            logger.info("Timing out active runs")
            self.update_states(timeout=True)
            self.status_updater.terminate()
            self.job_source.join()
            self.status_updater.join()

    def timeout_runs(self):
        for run in self.active_runs:
            run.terminate()


@click.command()
@click.option("--wall-time-min", type=int)
@click.option("--log-filename")
@click.option("--node-ids")
@click.option("--filter-tags")
def main(
    wall_time_min,
    log_filename,
    node_ids,
    filter_tags,
):
    site_config = SiteConfig()
    site_config.enable_logging("mpi_mode", filename=log_filename)
    filter_tags = json.loads(filter_tags)
    node_ids = json.loads(node_ids)

    node_cls = site_config.launcher.compute_node
    nodes = [node for node in node_cls.get_job_nodelist() if node.node_id in node_ids]
    node_manager = NodeManager(
        nodes, allow_node_packing=site_config.launcher.mpirun_allows_node_packing
    )

    batch_job_id = node_cls.get_batch_job_id()
    job_source = SynchronousJobSource(
        client=site_config.client,
        site_id=site_config.site_id,
        filter_tags=filter_tags,
        max_wall_time_min=wall_time_min,
        batch_job_id=batch_job_id,
    )
    status_updater = BulkStatusUpdater(site_config.client)

    App = site_config.client.App
    app_cache = {
        app.id: ApplicationDefinition.load_app_class(
            site_config.apps_path, app.class_path
        )
        for app in App.objects.filter(site_id=site_config.site_id)
    }
    launcher = Launcher(
        data_dir=site_config.data_path,
        app_cache=app_cache,
        idle_ttl_sec=site_config.launcher.idle_ttl_sec,
        delay_sec=site_config.launcher.delay_sec,
        app_run=site_config.launcher.mpi_app_launcher,
        node_manager=node_manager,
        job_source=job_source,
        status_updater=status_updater,
        wall_time_min=wall_time_min,
        error_tail_num_lines=site_config.launcher.error_tail_num_lines,
        max_concurrent_runs=site_config.launcher.max_concurrent_mpiruns,
    )
    launcher.run()


if __name__ == "__main__":
    main()
