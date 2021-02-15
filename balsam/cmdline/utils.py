import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, cast

import click
import psutil  # type: ignore

from balsam.config import SiteConfig
from balsam.schemas import BatchJobPartition

PID_FILENAME = "balsam-service.pid"


def list_to_dict(arg_list: List[str]) -> Dict[str, str]:
    return dict(cast(Tuple[str, str], arg.split("=", maxsplit=1)) for arg in arg_list)


def validate_tags(ctx: Any, param: Any, value: List[str]) -> Dict[str, str]:
    try:
        return list_to_dict(value)
    except ValueError:
        raise click.BadParameter("needs to be in format KEY=VALUE")


def validate_partitions(ctx: Any, param: Any, value: List[str]) -> List[BatchJobPartition]:
    partitions = []
    for arg in value:
        try:
            job_mode, num_nodes, *filter_tags_list = arg.split(":")
        except ValueError:
            raise click.BadParameter("needs to be in format MODE:NUM_NODES[:KEY=VALUE]")
        filter_tags: Dict[str, str] = validate_tags(ctx, param, filter_tags_list)
        partitions.append(BatchJobPartition(job_mode=job_mode, num_nodes=num_nodes, filter_tags=filter_tags))
    return partitions


def load_site_config() -> SiteConfig:
    try:
        cf = SiteConfig()
    except ValueError:
        raise click.BadParameter(
            "Cannot perform this action outside the scope of a Balsam Site. "
            "Please navigate into a Balsam site directory, or set "
            "BALSAM_SITE_PATH"
        )
    return cf


def get_pidfile(site_config: SiteConfig) -> Path:
    site_dir = site_config.site_path
    pid_file: Path = site_dir.joinpath(PID_FILENAME)
    return pid_file


def read_pidfile(site_config: SiteConfig) -> Tuple[str, int]:
    site_dir = site_config.site_path
    pid_file: Path = site_dir.joinpath(PID_FILENAME)
    service_host, service_pid = pid_file.read_text().split("\n")[:2]
    return service_host, int(service_pid)


def start_site(site_dir: Path) -> "subprocess.Popen[bytes]":
    os.environ["BALSAM_SITE_PATH"] = site_dir.as_posix()
    p = subprocess.Popen(
        [sys.executable, "-m", "balsam.site.service.main"],
        cwd=site_dir,
    )
    time.sleep(0.2)
    if p.poll() is None:
        return p
    raise RuntimeError(f"balsam.site.service.main return code {p.poll()}")


def check_killable(cf: SiteConfig, raise_exc: bool = False) -> Optional[int]:
    pid_file = get_pidfile(cf)
    cur_host = socket.gethostname()

    if not pid_file.is_file():
        if raise_exc:
            raise click.BadArgumentUsage(f"There is no {PID_FILENAME} in {cf.site_path}")
        return None
    try:
        service_host, service_pid = read_pidfile(cf)
    except ValueError:
        if raise_exc:
            raise click.BadArgumentUsage(
                f"Failed to read {pid_file}: please delete it and manually kill the site daemon process!"
            )
        return None
    if cur_host != service_host:
        if raise_exc:
            raise click.BadArgumentUsage(
                f"The site daemon is running on {service_host}; cannot stop from current host: {cur_host}"
            )
        return None
    if not psutil.pid_exists(service_pid):
        if raise_exc:
            raise click.BadArgumentUsage(
                f"Could not find process with PID {service_pid} on {service_host}. "
                f"Make sure the Balsam site daemon isn't running and delete {pid_file}"
            )
        return None
    return service_pid


def kill_site(cf: SiteConfig, service_pid: int) -> None:
    service_host = socket.gethostname()
    try:
        service_proc = psutil.Process(pid=service_pid)
        service_proc.terminate()
    except (ProcessLookupError, psutil.ProcessLookupError):
        raise click.BadArgumentUsage(
            f"Could not find process with PID {service_pid} on {service_host}. "
            f"Make sure the Balsam site daemon isn't running and delete {get_pidfile(cf)}"
        )
    click.echo(f"Sent SIGTERM to Balsam site daemon [pid {service_pid}]")
    click.echo("Waiting for site daemon to shutdown...")
    with click.progressbar(range(12)) as bar:
        for i in bar:
            try:
                service_proc.wait(timeout=1)
            except psutil.TimeoutExpired:
                if i == 11:
                    raise click.BadArgumentUsage(
                        f"Site daemon did not shut down gracefully on its own; please kill it manually "
                        f"and delete {get_pidfile(cf)}"
                    )
            else:
                click.echo("\nSite daemon shutdown OK")
                break
