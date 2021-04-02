import logging
import random
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, TextIO

import click
import yaml
from pydantic import BaseSettings, validator

from balsam.api import App, Job, JobState, Site
from balsam.schemas import RUNNABLE_STATES

logfmt = "%(asctime)s.%(msecs)03d | %(levelname)s | %(lineno)s] %(message)s"
logging.basicConfig(filename=None, level=logging.DEBUG, format=logfmt, datefmt="%Y-%m-%d %H:%M:%S", force=True)
logger = logging.getLogger()


class XPCS(BaseSettings):
    result_dir: Path
    remote_alias: str
    h5_in: Path
    imm_in: Path


class Eig(BaseSettings):
    remote_alias: str
    result_dir: Path
    matrix_in: Path


class ExperimentConfig(BaseSettings):
    experiment_tag: str
    submission_mode: str
    submit_period: float
    submit_batch_size: int
    max_site_backlog: int
    experiment_duration_min: int
    site_ids: List[int]
    app_name: str
    site_cpu_map: Dict[int, int]

    xpcs_datasets: List[XPCS]
    eig_datasets: List[Eig]

    @validator("submission_mode")
    def valid_submit_mode(cls, v: str) -> str:
        if v not in ["const-backlog", "shortest-backlog", "round-robin"]:
            raise ValueError(f"invalid mode: {v}")
        return v


class JobFactory:
    def __init__(
        self,
        experiment_tag: str,
        xpcs_datasets: List[XPCS],
        eig_datasets: List[Eig],
        site_cpu_map: Dict[int, int],
    ) -> None:
        self.idx = 0
        self.submission_idx = 0
        self.experiment_tag = experiment_tag
        self.xpcs_datasets = xpcs_datasets
        self.eig_datasets = eig_datasets
        self.generators: Dict[str, Callable[..., Job]] = {
            "xpcs.EigenCorr": self.xpcs_eigen,
            "eig.Eig": self.eig,
        }
        self.site_cpu_map = site_cpu_map

    def submit_jobs(self, app: App, num_jobs: int) -> List[Job]:
        job_factory = self.generators[app.class_path]
        jobs = [job_factory(app) for _ in range(num_jobs)]
        Job.objects.bulk_create(jobs)
        self.submission_idx += 1
        return jobs

    def xpcs_eigen(self, app: App) -> Job:
        assert app.id is not None

        transfer_set = random.choice(self.xpcs_datasets)
        source_tag = transfer_set.remote_alias

        workdir = Path(f"{self.experiment_tag}/{source_tag}/corr_{self.idx:06d}")
        result_path = transfer_set.result_dir.joinpath(transfer_set.h5_in.name).with_suffix(
            f".result{self.idx:06d}.hdf"
        )
        num_cpus = self.site_cpu_map[app.site_id]

        transfers: Dict[str, Any] = {
            "h5_in": {
                "location_alias": transfer_set.remote_alias,
                "path": transfer_set.h5_in,
            },
            "imm_in": {
                "location_alias": transfer_set.remote_alias,
                "path": transfer_set.imm_in,
            },
            "h5_out": {
                "location_alias": transfer_set.remote_alias,
                "path": result_path,
            },
        }
        job = Job(
            workdir=workdir,
            app_id=app.id,
            num_nodes=1,
            node_packing_count=1,
            threads_per_rank=num_cpus,
            transfers=transfers,
            tags={"job_source": source_tag, "experiment": self.experiment_tag},
        )
        self.idx += 1
        return job

    def eig(self, app: App) -> Job:
        assert app.id is not None

        transfer_set = random.choice(self.eig_datasets)
        source_tag = transfer_set.remote_alias

        workdir = Path(f"{self.experiment_tag}/{source_tag}/eig_{self.idx:06d}")
        result_path = transfer_set.result_dir.joinpath(transfer_set.matrix_in.name).with_suffix(
            f".eig{self.idx:06d}.npy"
        )

        transfers: Dict[str, Any] = {
            "matrix": {
                "location_alias": transfer_set.remote_alias,
                "path": transfer_set.matrix_in,
            },
            "eigvals": {
                "location_alias": transfer_set.remote_alias,
                "path": result_path,
            },
        }
        job = Job(
            workdir=workdir,
            app_id=app.id,
            num_nodes=1,
            node_packing_count=1,
            threads_per_rank=16,
            transfers=transfers,
            tags={"job_source": source_tag, "experiment": self.experiment_tag},
        )
        self.idx += 1
        return job


def get_site_backlogs(site_ids: List[int]) -> Dict[int, int]:
    backlogs: Dict[int, int] = {}
    for site_id in site_ids:
        qs = Job.objects.filter(site_id=site_id, state=set([*RUNNABLE_STATES, JobState.ready, JobState.staged_in]))
        count = qs.count()
        assert count is not None
        backlogs[site_id] = count
    return backlogs


def submit_const_backlog(
    job_factory: JobFactory,
    apps_by_site: Dict[int, App],
    backlogs_by_site: Dict[int, int],
    batch_size: int,
    max_backlog: int,
) -> None:
    for site_id, app in apps_by_site.items():
        backlog = backlogs_by_site[site_id]
        num_submit = min(batch_size, max_backlog - backlog)
        if num_submit < 1:
            logger.info(f"Site {site_id} at max_backlog: skipping")
        else:
            job_factory.submit_jobs(app, num_submit)
            logger.info(f"Submitted {num_submit} to Site {site_id}")


def submit_shortest_backlog(
    job_factory: JobFactory,
    apps_by_site: Dict[int, App],
    backlogs_by_site: Dict[int, int],
    batch_size: int,
    max_backlog: int,
) -> None:
    # Select Site with smallest backlog
    submit_site_id, backlog = min(backlogs_by_site.items(), key=lambda x: x[1])
    app = apps_by_site[submit_site_id]
    if backlog < max_backlog:
        job_factory.submit_jobs(app, batch_size)
        logger.info(f"Submitted {batch_size} jobs to Site {submit_site_id}")
    else:
        logger.info(f"Will not submit new jobs; at max backlog: {backlog} / {max_backlog}")


def submit_round_robin(
    job_factory: JobFactory,
    apps_by_site: Dict[int, App],
    backlogs_by_site: Dict[int, int],
    batch_size: int,
    max_backlog: int,
) -> None:
    # Select next Site in turn
    site_idx = job_factory.submission_idx % len(apps_by_site)
    site_id = list(apps_by_site.keys())[site_idx]
    app = apps_by_site[site_id]
    backlog = backlogs_by_site[site_id]
    logger.info(f"Backlogs by site: {backlogs_by_site}")
    if backlog < max_backlog:
        job_factory.submit_jobs(app, batch_size)
        logger.info(f"Submitted {batch_size} jobs to Site {site_id}")
    else:
        job_factory.submission_idx += 1
        logger.info(f"Will not submit new jobs to Site {site_id}; at max backlog: {backlog} / {max_backlog}")


@click.command()
@click.option("-c", "--config-file", required=True, type=click.File("r"))
def main(config_file: TextIO) -> None:
    config = ExperimentConfig(**yaml.safe_load(config_file))
    logger.debug(f"Loaded experiment config: {yaml.dump(config.dict(), sort_keys=False, indent=2)}")

    site_names = {site.id: (site.hostname, site.path.name) for site in Site.objects.filter(id=config.site_ids)}
    assert len(site_names) == len(
        config.site_ids
    ), f"Config specified site_ids {config.site_ids} but API only found {len(site_names)} of them."

    apps_by_site = {
        site_id: App.objects.get(site_id=site_id, class_path=config.app_name) for site_id in config.site_ids
    }

    job_factory = JobFactory(
        config.experiment_tag,
        config.xpcs_datasets,
        config.eig_datasets,
        config.site_cpu_map,
    )

    if config.submission_mode == "const-backlog":
        submit_method = submit_const_backlog
    elif config.submission_mode == "round-robin":
        submit_method = submit_round_robin
    elif config.submission_mode == "shortest-backlog":
        submit_method = submit_shortest_backlog
    else:
        raise ValueError("Invalid submission mode")

    start = datetime.utcnow()
    logger.info(f"Starting experiment at {start}")
    logger.info(f"Total duration will be {config.experiment_duration_min} minutes at most")

    while datetime.utcnow() - start < timedelta(minutes=config.experiment_duration_min):
        time.sleep(config.submit_period)
        backlogs = get_site_backlogs(config.site_ids)
        submit_method(
            job_factory,
            apps_by_site,
            backlogs,
            config.submit_batch_size,
            config.max_site_backlog,
        )

    logger.info("Reached experiment max duration, exiting.")


if __name__ == "__main__":
    main()
