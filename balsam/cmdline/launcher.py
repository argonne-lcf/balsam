import click
from datetime import datetime
from importlib.util import find_spec
import json
import os
import sys
import shlex
import subprocess

from balsam.cmdline.utils import load_site_config, validate_tags, validate_partitions
from balsam.site.launcher import NodeSpec

MPI_MODE_PATH = find_spec("balsam.site.launcher.mpi_mode").origin
SERIAL_MODE_PATH = find_spec("balsam.site.launcher.serial_mode").origin
PART_INDEX = 0


def get_run_basename(base):
    global PART_INDEX
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    fname = f"{base}_{timestamp}.{PART_INDEX}"
    PART_INDEX += 1
    return fname


def start_mpi_mode(site_config, wall_time_min, nodes, filter_tags):
    basename = get_run_basename("mpi_mode")
    log_filename = site_config.log_path.joinpath(basename + ".log")
    stdout_filename = site_config.log_path.joinpath(basename + ".out")
    node_ids = [n.node_id for n in nodes]

    env = os.environ.copy()
    env["BALSAM_LAUNCHER_NODES"] = str(len(nodes))
    env["BALSAM_JOB_MODE"] = "mpi"

    args = ""
    args += f"{sys.executable} {MPI_MODE_PATH} "
    args += f"--wall-time-min {wall_time_min} "
    args += f"--log-filename {log_filename} "
    args += f"--node-ids {json.dumps(node_ids)} "
    args += f"--filter-tags {json.dumps(filter_tags)} "

    proc = subprocess.Popen(
        args=shlex.split(args),
        env=env,
        stdout=open(stdout_filename, "wb"),
        stderr=subprocess.STDOUT,
    )
    return proc


def start_serial_mode(site_config, wall_time_min, nodes, filter_tags):
    master_host = nodes[0].hostname
    master_port = 19876
    basename = get_run_basename("serial_mode")
    log_filename = site_config.log_path.joinpath(basename + ".log")
    stdout_filename = site_config.log_path.joinpath(basename + ".out")
    node_spec = NodeSpec(
        node_ids=[n.node_id for n in nodes],
        hostnames=[n.hostname for n in nodes],
    )
    env = os.environ.copy()
    env["BALSAM_LAUNCHER_NODES"] = str(len(nodes))
    env["BALSAM_JOB_MODE"] = "serial"

    args = ""
    args += f"{sys.executable} {SERIAL_MODE_PATH} "
    args += f"--wall-time-min {wall_time_min} "
    args += f"--master-address {master_host}:{master_port} "
    args += f"--log-filename {log_filename} "
    args += f"--num-workers {len(nodes)} "
    args += f"--filter-tags {json.dumps(filter_tags)} "

    app_run = site_config.launcher.mpi_app_launcher
    app = app_run(
        cmdline=args,
        preamble=None,
        envs=env,
        cwd=os.getcwd(),
        outfile_path=stdout_filename,
        node_spec=node_spec,
        ranks_per_node=1,
        threads_per_rank=1,
        threads_per_core=1,
        launch_params=site_config.launcher.serial_mode_startup_params,
        gpus_per_rank=len(nodes[0].gpu_ids),
    )
    app.start()
    return app


@click.command()
@click.option("-j", "--job-mode", type=click.Choice(["serial", "mpi"]), required=True)
@click.option("-tag", "--tag", "filter_tags", multiple=True, callback=validate_tags)
@click.option("-p", "--part", "partitions", multiple=True, callback=validate_partitions)
@click.option("-t", "--wall-time-min", required=True, type=int)
def launcher(
    job_mode,
    filter_tags,
    partitions,
    wall_time_min,
):
    site_config = load_site_config()
    node_cls = site_config.launcher.compute_node
    nodes = node_cls.get_job_nodelist()

    if not partitions:
        partitions = [
            {"num_nodes": len(nodes), "job_mode": job_mode, "filter_tags": filter_tags}
        ]

    assert sum(p["num_nodes"] for p in partitions) == len(nodes)
    idx = 0
    for part in partitions:
        num_nodes = part.pop("num_nodes")
        start, end = idx, idx + num_nodes
        part["nodes"] = nodes[start:end]
        idx += num_nodes

    launcher_procs = []
    for part in partitions:
        job_mode = part.pop("job_mode")
        if job_mode == "mpi":
            proc = start_mpi_mode(
                site_config=site_config,
                wall_time_min=wall_time_min,
                **part,
            )
        else:
            proc = start_serial_mode(
                site_config=site_config,
                wall_time_min=wall_time_min,
                **part,
            )
        launcher_procs.append(proc)

    for proc in launcher_procs:
        proc.wait()


if __name__ == "__main__":
    launcher()
