import os
import socket
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, TypeVar, Union, cast

import click
import psutil  # type: ignore

from balsam._api.models import AppQuery, BatchJobQuery, JobQuery, Site
from balsam.config import ClientSettings, SiteConfig
from balsam.schemas import BatchJobPartition

if TYPE_CHECKING:
    from balsam.client import RESTClient


AppJobQuery = Union[AppQuery, JobQuery, BatchJobQuery]
T = TypeVar("T", AppQuery, JobQuery, BatchJobQuery)

PID_FILENAME = "balsam-service.pid"


def utc_past(minutes_ago: int) -> datetime:
    return datetime.utcnow() - timedelta(minutes=minutes_ago)


def is_site_active(s: Site, threshold_min: int = 2) -> bool:
    threshold = utc_past(minutes_ago=threshold_min)
    if s.last_refresh is None:
        return False
    return s.last_refresh >= threshold


def filter_by_sites(query: T, site_str: str = "") -> T:
    """
    Applies the appropriate site_id filter to a query based on site selector string.
    - --site=all does not filter by site_id
    - --site=this explicitly filters by local site_id
    - --site=active filters by active sites only
    - otherwise, a comma-separated list of ID's or Path fragments
    Default behavior (no --site argument):
        - Select the local site_id if available
        - Otherwise, select all active sites
    """
    Site = query._manager._client.Site
    try:
        site_conf: Optional[SiteConfig] = SiteConfig()
    except ValueError:
        site_conf = None
    site_id: Optional[int] = site_conf.site_id if site_conf else None

    values = [s.strip() for s in site_str.split(",") if s.strip()]
    active_t = utc_past(minutes_ago=2)
    if not values:
        if site_id is not None:
            return query.filter(site_id=site_id)
        else:
            active_ids = [site.id for site in Site.objects.filter(last_refresh_after=active_t) if site.id]
            return query.filter(site_id=active_ids)

    if "all" in values:
        return query
    if "active" in values:
        active_ids = [site.id for site in Site.objects.filter(last_refresh_after=active_t) if site.id]
        return query.filter(site_id=active_ids)
    if "this" in values:
        if site_id is None:
            raise click.BadParameter(
                "Cannot use --site=this outside of a Balsam Site. "
                "Please navigate into a Balsam site directory, or set "
                "BALSAM_SITE_PATH"
            )
        return query.filter(site_id=site_id)

    site_ids, path_fragments = [], []
    for v in values:
        if v.isdigit():
            site_ids.append(int(v))
        else:
            path_fragments.append(v)

    for path_fragment in path_fragments:
        site_ids.extend(site.id for site in Site.objects.filter(path=path_fragment) if site.id)

    return query.filter(site_id=site_ids)


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


def load_site_from_selector(site_selector: str) -> Site:
    client = load_client()
    Site = client.Site
    if not site_selector:
        site_config: SiteConfig = load_site_config()
        site = Site.objects.get(id=site_config.site_id)
    elif site_selector.isdigit():
        site = Site.objects.get(id=int(site_selector))
    else:
        site = Site.objects.get(path=site_selector)
    return site


def load_client() -> "RESTClient":
    return ClientSettings.load_from_file().build_client()


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
    click.echo(f"Waiting for site {cf.site_path} to shutdown...")
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


def table_print(data: List[Dict[str, Any]]) -> None:
    if not data:
        return
    col_names = list(data[0].keys())
    max_widths = [len(header) + 2 for header in col_names]
    rows = []
    for record in data:
        row = [str(record[col_name]) for col_name in col_names]
        for i, entry in enumerate(row):
            max_widths[i] = max(max_widths[i], len(entry) + 2)
        rows.append(row)

    header = " ".join(col_name.ljust(width) for col_name, width in zip(col_names, max_widths))
    click.echo(header)

    for row in rows:
        line = " ".join(row[i].ljust(width) for i, width in enumerate(max_widths))
        click.echo(line)
