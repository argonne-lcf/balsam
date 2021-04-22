import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Type, Union, cast

import click

from balsam.config import SiteConfig
from balsam.platform import TimeoutExpired
from balsam.schemas import JobState
from balsam.site import ApplicationDefinition, BulkStatusUpdater, SynchronousJobSource
from balsam.site.launcher.node_manager import NodeManager
from balsam.site.launcher.util import countdown_timer_min
from balsam.util import SigHandler

logger = logging.getLogger("balsam.site.launcher.mpi_mode")

if TYPE_CHECKING:
    from balsam._api.models import Job
    from balsam.platform.app_run import AppRun


class Launcher:
    def __init__(
        self,
        data_dir: Path,
        app_cache: Dict[int, Type[ApplicationDefinition]],
        idle_ttl_sec: int,
        app_run: Type["AppRun"],
        node_manager: NodeManager,
        job_source: SynchronousJobSource,
        status_updater: BulkStatusUpdater,
        wall_time_min: int,
        delay_sec: int,
        error_tail_num_lines: int,
        max_concurrent_runs: int,
    ) -> None:
        self.data_dir = data_dir
        self.app_cache = app_cache
        self.idle_ttl_sec = idle_ttl_sec
        self.error_tail_num_lines = error_tail_num_lines
        self.app_run = app_run
        self.node_manager = node_manager
        self.job_source = job_source
        self.status_updater = status_updater
        self.timer = countdown_timer_min(max(1, wall_time_min - 2), delay_sec)

        self.status_updater.start()
        self.job_source.start()
        self.active_runs: Dict[int, "AppRun"] = {}
        self.idle_time: Optional[float] = None
        self.max_concurrent_runs = max_concurrent_runs

        self.sig_handler = SigHandler()

    def time_step(self) -> None:
        try:
            min_left = next(self.timer)
        except StopIteration:
            self.sig_handler.set()
            return

        m, s = map(int, divmod(min_left * 60, 60))
        logger.debug(f"{m:02d}m:{s:02d}s remaining")

    def check_exit(self) -> None:
        if not self.active_runs:
            if self.idle_time is None:
                self.idle_time = time.time()
            elif time.time() - self.idle_time > self.idle_ttl_sec:
                self.sig_handler.set()
                logger.info(f"Exceeded {self.idle_ttl_sec} sec TTL: shutting down because nothing to do")
        else:
            self.idle_time = None

    def start_job(self, job: "Job") -> "AppRun":
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
            gpus_per_rank=int(job.gpus_per_rank),
        )
        return run

    def launch_runs(self) -> None:
        max_nodes_per_job = self.node_manager.count_empty_nodes()
        max_aggregate_nodes = self.node_manager.aggregate_free_nodes()
        max_num_to_acquire = max(0, self.max_concurrent_runs - len(self.active_runs))
        if not self.node_manager.allow_node_packing:
            max_num_to_acquire = min(max_num_to_acquire, int(max_aggregate_nodes))
        if max_aggregate_nodes < 0.01:
            return

        acquired = self.job_source.get_jobs(
            max_num_jobs=max_num_to_acquire,
            max_nodes_per_job=max_nodes_per_job,
            max_aggregate_nodes=max_aggregate_nodes,
        )
        if acquired:
            logger.info(
                f"Job Acqusition: {max_nodes_per_job} empty nodes; {max_aggregate_nodes} aggregate free nodes; "
                f"requested up to {max_num_to_acquire} jobs [node packing allowed: {self.node_manager.allow_node_packing}]; "
                f"Acquired {len(acquired)} jobs."
            )
        for job in acquired:
            run = self.start_job(job)
            run.start()
            self.status_updater.put(
                cast(int, job.id),  # acquired jobs will not have None id
                state=JobState.running,
                state_timestamp=datetime.utcnow(),
                state_data={"num_nodes": float(job.num_nodes) / job.node_packing_count},
            )
            self.active_runs[cast(int, job.id)] = run

    def check_run(self, run: "AppRun") -> Dict[str, Any]:
        retcode = run.poll()
        if retcode is None:
            return {"state": "RUNNING"}
        elif retcode == 0:
            return {"state": "RUN_DONE", "state_timestamp": datetime.utcnow()}
        else:
            tail = run.tail_output(nlines=self.error_tail_num_lines)
            logger.info(f"Run error: {tail}")
            return {
                "state": "RUN_ERROR",
                "state_timestamp": datetime.utcnow(),
                "state_data": {"returncode": retcode, "error": tail},
            }

    @staticmethod
    def timeout_kill(runs: Iterable["AppRun"], timeout: float = 10) -> None:
        start = time.time()
        for run in runs:
            remaining = max(0, timeout - (time.time() - start))
            try:
                run.wait(timeout=remaining)
            except TimeoutExpired:
                run.kill()

    def update_states(self, timeout: bool = False) -> None:
        remaining_runs = {}
        for id, run in self.active_runs.items():
            status = self.check_run(run)
            if status["state"] == "RUNNING" and timeout:
                run.terminate()
                self.status_updater.put(id, state=JobState.run_timeout, state_timestamp=datetime.utcnow())
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

    def run(self) -> None:
        try:
            while not self.sig_handler.is_set():
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

    def timeout_runs(self) -> None:
        for run in self.active_runs.values():
            run.terminate()


@click.command()
@click.option("--wall-time-min", type=int)
@click.option("--log-filename")
@click.option("--node-ids")
@click.option("--filter-tags")
def main(
    wall_time_min: int,
    log_filename: str,
    node_ids: str,
    filter_tags: str,
) -> None:
    site_config = SiteConfig()
    site_config.enable_logging("mpi_mode", filename=log_filename)
    filter_tags_dict: Dict[str, str] = json.loads(filter_tags)
    node_ids_list: List[Union[int, str]] = json.loads(node_ids)
    if filter_tags_dict:
        logger.info(f"Launcher filtering for tags: {filter_tags_dict}")

    launch_settings = site_config.settings.launcher
    node_cls = launch_settings.compute_node
    nodes = [node for node in node_cls.get_job_nodelist() if node.node_id in node_ids_list]
    node_manager = NodeManager(nodes, allow_node_packing=launch_settings.mpirun_allows_node_packing)

    App = site_config.client.App
    app_cache = {
        app.id: ApplicationDefinition.load_app_class(site_config.apps_path, app.class_path)
        for app in App.objects.filter(site_id=site_config.site_id)
        if app.id is not None
    }

    scheduler_id = node_cls.get_scheduler_id()
    job_source = SynchronousJobSource(
        client=site_config.client,
        site_id=site_config.site_id,
        filter_tags=filter_tags_dict,
        max_wall_time_min=wall_time_min,
        scheduler_id=scheduler_id,
        app_ids={app_id for app_id in app_cache if app_id is not None},
    )
    status_updater = BulkStatusUpdater(site_config.client)

    launcher = Launcher(
        data_dir=site_config.data_path,
        app_cache=app_cache,
        idle_ttl_sec=launch_settings.idle_ttl_sec,
        delay_sec=launch_settings.delay_sec,
        app_run=launch_settings.mpi_app_launcher,
        node_manager=node_manager,
        job_source=job_source,
        status_updater=status_updater,
        wall_time_min=wall_time_min,
        error_tail_num_lines=launch_settings.error_tail_num_lines,
        max_concurrent_runs=launch_settings.max_concurrent_mpiruns,
    )
    launcher.run()
