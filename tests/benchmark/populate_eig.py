import random
import time
from pathlib import Path
from typing import Any, Dict, List

import click

from balsam.api import App, Job, Site

APP_CLASS = "eig.Eig"
SITE_ID = 11
REMOTE_ALIAS = "aps_dtn"
RESULT_DIR: Path = Path("/gdata/lcfwork/results-2021-03-24/")
SITE = Site.objects.get(id=SITE_ID)
assert SITE.id is not None
APP = App.objects.get(site_id=SITE.id, class_path=APP_CLASS)

SIZE_TO_FILES = {
    "S": ["/gdata/lcfwork/eig-inputs/5000.npy"],
    "L": ["/gdata/lcfwork/eig-inputs/12_000.npy"],
    "C": ["/gdata/lcfwork/eig-inputs/5000.npy", "/gdata/lcfwork/eig-inputs/12_000.npy"],
}


def populate_jobs(num_nodes: int, size_tag: str, num_jobs: int, exp_tag: str) -> None:
    tags = {
        "experiment": exp_tag,
        "job_source": REMOTE_ALIAS,
        "num_nodes": str(num_nodes),
        "size": size_tag,
    }
    jobs: List[Job] = []
    for i in range(num_jobs):
        workdir = Path(exp_tag) / (str(num_nodes) + str(size_tag)) / f"{i:06d}"
        inp_path = Path(random.choice(SIZE_TO_FILES[size_tag]))
        assert inp_path.is_absolute()
        result_path = RESULT_DIR.joinpath(inp_path.name).with_suffix(f".eig{i:06d}.npy")
        transfers: Dict[str, Any] = {
            "matrix": {
                "location_alias": REMOTE_ALIAS,
                "path": inp_path,
            },
            "eigvals": {
                "location_alias": REMOTE_ALIAS,
                "path": result_path,
            },
        }
        assert APP.id is not None
        job = Job(
            app_id=APP.id,
            workdir=workdir,
            tags=tags,
            parameters={"inp_file": inp_path.name},
            transfers=transfers,
            threads_per_rank=64,
            launch_params={"cpu_affinity": "depth"},
        )
        jobs.append(job)
    Job.objects.bulk_create(jobs)


@click.command()
@click.option("-t", "--time-limit-min", type=int, required=True)
@click.option("-n", "--num-nodes", type=int, required=True)
@click.option("-s", "--size", type=click.Choice(["L", "C"]), required=True)
@click.option("-e", "--experiment", required=True)
def main(time_limit_min: int, num_nodes: int, size: str, experiment: str) -> None:
    start = time.time()
    time_limit_sec = time_limit_min * 60
    tags = {
        "experiment": experiment,
        "job_source": REMOTE_ALIAS,
        "num_nodes": str(num_nodes),
        "size": size,
    }
    while (time.time() - start) < time_limit_sec:
        qs = Job.objects.filter(app_id=APP.id, tags=tags, state="READY")  # type: ignore
        to_create = max(0, 48 - qs.count())  # type: ignore
        if to_create:
            populate_jobs(num_nodes=num_nodes, size_tag=size, num_jobs=to_create, exp_tag=experiment)
            print(f"Added {to_create} jobs to pipeline: {tags}")
        else:
            print("Pipeline already full; not adding jobs")
        time.sleep(2 + random.random())
    print("Done!")


if __name__ == "__main__":
    main()
