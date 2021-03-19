import logging
import random
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, TextIO, Tuple

import click
import yaml
from pydantic import BaseSettings

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
    submit_period_range_sec: Tuple[int, int]
    submit_batch_size_range: Tuple[int, int]
    max_site_backlog: int
    experiment_duration_min: int
    site_ids: List[int]
    app_name: str

    xpcs_datasets: List[XPCS]
    eig_datasets: List[Eig]


class JobFactory:
    def __init__(
        self,
        experiment_tag: str,
        batch_size_range: Tuple[int, int],
        xpcs_datasets: List[XPCS],
        eig_datasets: List[Eig],
    ) -> None:
        self.idx = 0
        self.experiment_tag = experiment_tag
        self.batch_size_range = batch_size_range
        self.xpcs_datasets = xpcs_datasets
        self.eig_datasets = eig_datasets
        self.generators: Dict[str, Callable[..., Job]] = {
            "xpcs.EigenCorr": self.xpcs_eigen,
            "eig.Eig": self.eig,
        }

    def submit_jobs(self, app: App) -> List[Job]:
        job_factory = self.generators[app.class_path]
        num_jobs = random.randint(*self.batch_size_range)
        jobs = [job_factory(app) for _ in range(num_jobs)]
        Job.objects.bulk_create(jobs)
        return jobs

    def xpcs_eigen(self, app: App) -> Job:
        assert app.id is not None

        transfer_set = random.choice(self.xpcs_datasets)
        source_tag = transfer_set.remote_alias

        workdir = Path(f"{self.experiment_tag}/{source_tag}/corr_{self.idx:06d}")
        result_path = transfer_set.result_dir.joinpath(transfer_set.h5_in.name).with_suffix(
            f"result{self.idx:06d}.hdf"
        )

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
            threads_per_rank=16,
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
            f"eig{self.idx:06d}.npy"
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
        config.submit_batch_size_range,
        config.xpcs_datasets,
        config.eig_datasets,
    )

    start = datetime.utcnow()
    logger.info(f"Starting experiment at {start}")
    logger.info(f"Total duration will be {config.experiment_duration_min} minutes at most")

    while datetime.utcnow() - start < timedelta(minutes=config.experiment_duration_min):
        sleep_time = random.randint(*config.submit_period_range_sec)
        time.sleep(sleep_time)

        backlogs: Dict[int, int] = {
            site_id: Job.objects.filter(site_id=site_id, state=set([*RUNNABLE_STATES, JobState.ready])).count()  # type: ignore
            for site_id in config.site_ids
        }

        # Load level: submit to Site with smallest backlog
        submit_site_id, backlog = min(backlogs.items(), key=lambda x: x[1])

        if backlog < config.max_site_backlog:
            jobs = job_factory.submit_jobs(apps_by_site[submit_site_id])
            logger.info(f"Submitted {len(jobs)} jobs to Site {site_names[submit_site_id]}")
        else:
            logger.info("Will not submit new jobs; at max backlog: " f"{backlog} / {config.max_site_backlog}")

    logger.info("Reached experiment max duration, exiting.")


if __name__ == "__main__":
    main()
