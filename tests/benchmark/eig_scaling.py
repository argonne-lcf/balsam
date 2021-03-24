import random
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

import yaml
from pydantic import BaseSettings

from balsam.api import App, BatchJob, Job, Site
from balsam.schemas import JobState

APP_CLASS = "eig.Eig"
RESULT_IDX = 0
ACTIVE_STATES = set(
    [
        JobState.created,
        JobState.ready,
        JobState.awaiting_parents,
        JobState.staged_in,
        JobState.preprocessed,
        JobState.running,
    ]
)


class RunConf(BaseSettings):
    time_limit_min: int = 16
    remote_alias: str = "aps_dtn"
    result_dir: Path = Path("/gdata/lcfwork/results-2021-03-24/")
    num_nodes: int
    jobs_per_sec: float
    matrix_inputs: List[Path]
    size_tag: str


class Conf(BaseSettings):
    project: str = "WorkExpFacil"
    queue: str = "R.WorkExpFacil"
    site_id: int = 11
    experiment_tag: str
    run_list: List[RunConf]


def submit_eig_job(
    app_id: int, workdir: Path, tags: Dict[str, str], remote_alias: str, matrix_inp_path: Path, result_dir: Path
) -> Job:
    result_path = result_dir.joinpath(matrix_inp_path.name).with_suffix(f".eig{RESULT_IDX:06d}.npy")
    transfers: Dict[str, Any] = {
        "matrix": {
            "location_alias": remote_alias,
            "path": matrix_inp_path,
        },
        "eigvals": {
            "location_alias": remote_alias,
            "path": result_path,
        },
    }
    j = Job(
        app_id=app_id,
        workdir=workdir,
        tags=tags,
        parameters={"inp_file": matrix_inp_path.name},
        transfers=transfers,
        threads_per_rank=64,
        launch_params={"cpu_affinity": "depth"},
    )
    j.save()
    return j


def run(
    site_id: int,
    app_id: int,
    project: str,
    queue: str,
    experiment_tag: str,
    num_nodes: int,
    time_limit_min: int,
    jobs_per_sec: float,
    remote_alias: str,
    result_dir: Path,
    matrix_inputs: List[Path],
    size_tag: str,
) -> None:
    global RESULT_IDX
    tags = {
        "experiment": experiment_tag,
        "job_source": remote_alias,
        "num_nodes": str(num_nodes),
        "size": size_tag,
    }
    tag_qs = [f"{k}:{v}" for k, v in tags.items()]
    sleep_time = 1.0 / jobs_per_sec
    start = datetime.utcnow()
    batch_job = BatchJob.objects.create(
        num_nodes=num_nodes,
        wall_time_min=time_limit_min,
        job_mode="mpi",  # type: ignore
        site_id=site_id,
        project=project,
        queue=queue,
        filter_tags=tags,
    )
    print("Created BatchJob", batch_job)
    while datetime.utcnow() - start < timedelta(minutes=time_limit_min + 1):
        time.sleep(sleep_time)
        active_jobs = Job.objects.filter(app_id=app_id, tags=tag_qs, state=ACTIVE_STATES)
        num_active = active_jobs.count()
        assert num_active is not None

        if num_active < num_nodes:
            workdir = Path(experiment_tag) / (str(num_nodes) + str(size_tag)) / f"{RESULT_IDX:06d}"
            inp_path = random.choice(matrix_inputs)
            job = submit_eig_job(
                app_id=app_id,
                workdir=workdir,
                tags=tags,
                remote_alias=remote_alias,
                matrix_inp_path=inp_path,
                result_dir=result_dir,
            )
            print(f"Submitted job {job.id} to process {inp_path.name}")
            RESULT_IDX += 1

    print("Run Done, waiting for BatchJob to finish")
    batch_job.refresh_from_db()
    i = 0
    while batch_job.state != "finished" and i < 6:
        time.sleep(10)
        batch_job.refresh_from_db()
        i += 1
    print(f"Finished waiting on BatchJob (state {batch_job.state}); Moving on to next run")


def main() -> None:
    with open("eig_scaling.yml") as fp:
        conf = Conf(**yaml.safe_load(fp))

    site = Site.objects.get(id=conf.site_id)
    app = App.objects.get(site_id=site.id, class_path=APP_CLASS)
    assert site.id is not None
    assert app.id is not None

    for run_conf in conf.run_list:
        print("Starting experiment:", run_conf)
        run(
            site_id=site.id,
            app_id=app.id,
            project=conf.project,
            queue=conf.queue,
            experiment_tag=conf.experiment_tag,
            **run_conf.dict(),
        )


if __name__ == "__main__":
    main()
