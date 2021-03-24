import os
import random
import time
from datetime import datetime
from pathlib import Path
from pprint import pprint
from typing import List

import click
import jinja2

from balsam.platform.scheduler import CobaltScheduler

os.environ["BALSAM_SITE_PATH"] = "/tmp/fakesite"

SUBMIT_PROJECT = "WorkExpFacil"
SUBMIT_QUEUE = "R.WorkExpFacil"
RUN_COMMAND = "aprun -n 1 -d 64 -cc depth python /projects/WorkExpFacil/msalim/eig/random-eig.py "
sched = CobaltScheduler()

sh_template = jinja2.Template(
    """
#!/bin/bash
echo START_RUN: `date --iso-8601=ns`
cp {{ input_file }} .
echo COPIED_INPUT: `date --iso-8601=ns`
module load miniconda-3
export OMP_NUM_THREADS=64
{{ run_cmd }}
echo RUN_DONE: `date --iso-8601=ns`
cp *.eigs.npy {{ result_dir }}
echo COPIED_OUTPUT: `date --iso-8601=ns`
    """.strip()
)


def render_jobscript(input_file: Path, run_cmd: str, result_dir: Path) -> str:
    return sh_template.render(locals())


def submit_job(input_file: Path, workdir: Path, result_dir: Path) -> int:
    workdir.mkdir(parents=True, exist_ok=False)
    script = render_jobscript(
        input_file=input_file,
        run_cmd=RUN_COMMAND + " " + input_file.name,
        result_dir=result_dir,
    )
    with open(workdir / "job.sh", "w") as fp:
        fp.write(script)
    os.chmod(fp.name, 0o750)

    job_id = sched.submit(
        fp.name,
        project=SUBMIT_PROJECT,
        queue=SUBMIT_QUEUE,
        num_nodes=1,
        wall_time_min=5,
        cwd=workdir,
    )
    return job_id


@click.command()
@click.option("-n", "--num-nodes", required=True, type=int)
@click.option("-t", "--time-min", required=True, type=int)
@click.option("-jps", "--jobs-per-sec", required=True, type=float)
@click.option(
    "-i", "--input-file", "input_files", required=True, type=click.Path(file_okay=True, exists=True), multiple=True
)
@click.option("-e", "exp_dir", type=click.Path(exists=False), required=True)
def main(num_nodes: int, time_min: int, jobs_per_sec: float, input_files: List[Path], exp_dir: Path) -> None:
    """
    Runs batch-queue simulation
    """
    exp_dir = Path(exp_dir)
    exp_dir.mkdir(parents=True, exist_ok=False)
    result_dir = exp_dir.joinpath("results")
    result_dir.mkdir()

    input_files = [Path(inp).resolve() for inp in input_files]

    submit_delay = 1.0 / jobs_per_sec
    check_interval = 5.0
    check_steps = int(check_interval / submit_delay)
    nstep = int(time_min * 60 / submit_delay)
    pprint(locals())

    active_job_ids: List[int] = []

    for i in range(nstep):
        if len(active_job_ids) < num_nodes:
            input_file = random.choice(input_files)
            workdir = exp_dir.joinpath(input_file.with_suffix("").name).joinpath(f"{i:04d}")
            job_id = submit_job(input_file, workdir, result_dir)
            print("SUBMITTED:", workdir, datetime.now().isoformat())
            active_job_ids.append(job_id)

        if i % check_steps == 0:
            start = time.time()
            scheduler_ids = list(sched.get_statuses().keys())
            active_job_ids = [job_id for job_id in active_job_ids if job_id in scheduler_ids]
            sched_time = time.time() - start
            sleep_time = submit_delay - sched_time
        else:
            sleep_time = submit_delay
        if sleep_time > 0.0:
            time.sleep(sleep_time)


if __name__ == "__main__":
    main()
