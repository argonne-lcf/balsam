import random
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict

from pydantic import BaseSettings

from balsam.api import App, BatchJob, Job, Site
from balsam.schemas import BatchJobState

APP_CLASS = "eig.Eig"
RESULT_IDX = 0


class Conf(BaseSettings):
    project: str = "WorkExpFacil"
    queue: str = "R.WorkExpFacil"
    site_id: int = 11
    experiment_tag: str = "eig-backlog-theta1"

    remote_alias: str = "aps_dtn"
    result_dir: Path = Path("/gdata/lcfwork/results-2021-03-24/")
    matrix_input: Path = Path("/gdata/lcfwork/eig-inputs/5000.npy")


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
    experiment_tag: str,
    time_limit_min: int,
    jobs_per_sec: float,
    remote_alias: str,
    result_dir: Path,
    matrix_input: Path,
    random_kill: bool = False,
) -> None:
    global RESULT_IDX
    tags = {
        "experiment": experiment_tag,
        "job_source": remote_alias,
    }
    sleep_time = 1.0 / jobs_per_sec
    print("Will sleep for ", sleep_time, "between submissions")
    start = datetime.utcnow()
    last_kill = datetime.utcnow()

    while datetime.utcnow() - start < timedelta(minutes=time_limit_min):
        time.sleep(sleep_time)
        workdir = Path(experiment_tag) / f"{RESULT_IDX:06d}"
        job = submit_eig_job(
            app_id=app_id,
            workdir=workdir,
            tags=tags,
            remote_alias=remote_alias,
            matrix_inp_path=matrix_input,
            result_dir=result_dir,
        )
        print(f"Submitted job {job.id}")
        RESULT_IDX += 1

        if random_kill and (datetime.utcnow() - last_kill) >= timedelta(minutes=2):
            site_bjobs = list(BatchJob.objects.filter(state="running", site_id=site_id))
            if site_bjobs:
                bjob = random.choice(site_bjobs)
                bjob.state = BatchJobState.pending_deletion
                bjob.save()
                print("Killing BatchJob", bjob.id)
                last_kill = datetime.utcnow()


def main() -> None:
    conf = Conf()

    site = Site.objects.get(id=conf.site_id)
    app = App.objects.get(site_id=site.id, class_path=APP_CLASS)
    assert site.id is not None
    assert app.id is not None

    print("Starting steady run")
    run(
        site_id=site.id,
        app_id=app.id,
        experiment_tag=conf.experiment_tag,
        jobs_per_sec=1,
        time_limit_min=15,
        remote_alias=conf.remote_alias,
        result_dir=conf.result_dir,
        matrix_input=conf.matrix_input,
    )

    print("Starting too-fast run")
    run(
        site_id=site.id,
        app_id=app.id,
        experiment_tag=conf.experiment_tag,
        jobs_per_sec=3,
        time_limit_min=15,
        remote_alias=conf.remote_alias,
        result_dir=conf.result_dir,
        matrix_input=conf.matrix_input,
    )

    print("Starting random-kill  run")
    run(
        site_id=site.id,
        app_id=app.id,
        experiment_tag=conf.experiment_tag,
        jobs_per_sec=1,
        time_limit_min=15,
        random_kill=True,
        remote_alias=conf.remote_alias,
        result_dir=conf.result_dir,
        matrix_input=conf.matrix_input,
    )
    print("ALL DONE; waiting for backlog to finish normally")


if __name__ == "__main__":
    main()
