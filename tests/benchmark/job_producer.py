import logging
import random
import time
from datetime import datetime, timedelta
from itertools import product
from pathlib import Path
from typing import Any, Callable, Dict, List, TextIO, Tuple

import click
import yaml
from pydantic import BaseSettings

from balsam.api import App, Job, JobState, Site

logfmt = "%(asctime)s.%(msecs)03d | %(levelname)s | %(lineno)s] %(message)s"
logging.basicConfig(filename=None, level=logging.DEBUG, format=logfmt, datefmt="%Y-%m-%d %H:%M:%S", force=True)
logger = logging.getLogger()


class XPCSTransferSet(BaseSettings):
    h5_in: Path
    imm_in: Path


class XPCSConfig(BaseSettings):
    result_dir: Path
    remote_alias: str
    transfer_sets: List[XPCSTransferSet]


class ExperimentConfig(BaseSettings):
    experiment_tag: str
    source_tag: str
    submit_period_range_sec: Tuple[int, int]
    submit_batch_size_range: Tuple[int, int]
    max_transfer_backlog: int
    experiment_duration_min: int
    site_ids: List[int]
    app_names: List[str]

    xpcs_config: XPCSConfig


class JobFactory:
    def __init__(
        self,
        experiment_tag: str,
        source_tag: str,
        batch_size_range: Tuple[int, int],
        xpcs_config: XPCSConfig,
    ) -> None:
        self.idx = 0
        self.experiment_tag = experiment_tag
        self.source_tag = source_tag
        self.batch_size_range = batch_size_range
        self.xpcs_config = xpcs_config
        self.generators: Dict[str, Callable[..., Job]] = {
            "demo.Hello": self.hello_world,
            "xpcs.EigenCorr": self.xpcs_eigen,
        }

    def submit_jobs(self, app: App) -> List[Job]:
        job_factory = self.generators[app.class_path]
        num_jobs = random.randint(*self.batch_size_range)
        jobs = [job_factory(app) for _ in range(num_jobs)]
        Job.objects.bulk_create(jobs)
        return jobs

    def hello_world(self, app: App) -> Job:
        assert app.id is not None
        workdir = Path(f"{self.experiment_tag}/{self.source_tag}/hello_{self.idx:06d}")
        job = Job(
            workdir=workdir,
            num_nodes=1,
            node_packing_count=64,
            app_id=app.id,
            parameters={"name": f"world {self.idx}!"},
            tags={"job_source": self.source_tag, "experiment": self.experiment_tag},
        )
        self.idx += 1
        return job

    def xpcs_eigen(self, app: App) -> Job:
        assert app.id is not None
        workdir = Path(f"{self.experiment_tag}/{self.source_tag}/corr_{self.idx:06d}")
        transfer_set = random.choice(self.xpcs_config.transfer_sets)
        h5_name = transfer_set.h5_in.name
        transfers: Dict[str, Any] = {
            key: {
                "location_alias": self.xpcs_config.remote_alias,
                "path": path,
            }
            for key, path in transfer_set.dict().items()
        }
        transfers["h5_out"] = {
            "location_alias": self.xpcs_config.remote_alias,
            "path": Path(self.xpcs_config.result_dir).joinpath(h5_name),
        }
        job = Job(
            workdir=workdir,
            app_id=app.id,
            num_nodes=1,
            node_packing_count=1,
            threads_per_rank=64,
            transfers=transfers,
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

    apps = {
        (site_id, app_name): App.objects.get(site_id=site_id, class_path=app_name)
        for (site_id, app_name) in product(config.site_ids, config.app_names)
    }

    job_factory = JobFactory(
        config.experiment_tag,
        config.source_tag,
        config.submit_batch_size_range,
        config.xpcs_config,
    )

    start = datetime.utcnow()
    logger.info(f"Starting experiment at {start}")
    logger.info("Total duration will be {config.experiment_duration_min} minutes at most")

    while datetime.utcnow() - start < timedelta(minutes=config.experiment_duration_min):
        sleep_time = random.randint(*config.submit_period_range_sec)
        time.sleep(sleep_time)

        for (site_id, app_name), app in apps.items():
            backlog = Job.objects.filter(site_id=site_id, state=JobState.ready).count()
            assert backlog is not None
            if backlog < config.max_transfer_backlog:
                jobs = job_factory.submit_jobs(app)
                logger.info(f"Submitted {len(jobs)} {app_name} jobs to Site {site_names[site_id]}")

    logger.info("Reached experiment max duration, exiting.")


if __name__ == "__main__":
    main()
