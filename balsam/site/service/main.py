import logging
import multiprocessing
import os
import signal
import socket
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, cast

from pydantic import AnyUrl

from balsam.config import Settings, SiteConfig

if TYPE_CHECKING:
    from balsam._api.models import Site

logger = logging.getLogger("balsam.site.service.main")

EXIT_FLAG = False


def handler(signum: int, stack: Any) -> None:
    global EXIT_FLAG
    EXIT_FLAG = True


class PIDFile:
    def __init__(self, site_path: Path) -> None:
        self.path: Path = site_path.joinpath("balsam-service.pid")

    def __enter__(self) -> None:
        if self.path.exists():
            raise RuntimeError(
                f"{self.path} already exists: will not start Balsam service because it's already running. "
                f"To restart, first stop the Balsam service with `balsam service stop`"
            )
        with open(self.path, "w") as fp:
            fp.write(f"{socket.gethostname()}\n{os.getpid()}")

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.path.unlink()


def update_site_from_config(site: "Site", settings: Settings) -> None:
    old_dict = site.display_dict()
    if settings.scheduler:
        site.allowed_projects = settings.scheduler.allowed_projects
        site.allowed_queues = settings.scheduler.allowed_queues
        site.optional_batch_job_params = settings.scheduler.optional_batch_job_params
    if settings.transfers:
        site.transfer_locations = cast(Dict[str, AnyUrl], settings.transfers.transfer_locations)
        site.globus_endpoint_id = settings.transfers.globus_endpoint_id

    new_dict = site.display_dict()
    diff = {k: (old_dict[k], new_dict[k]) for k in old_dict if old_dict[k] != new_dict[k]}
    if diff:
        site.save()
        diff_str = "\n".join(f"{k}={diff[k][0]} --> {diff[k][1]}" for k in diff)
        logger.info(f"Updated Site parameters:\n{diff_str}")


def main(config: SiteConfig, run_time_sec: int) -> None:
    start_time = time.time()
    config.enable_logging(basename="service")
    m, s = divmod(run_time_sec, 60)
    h, m = divmod(m, 60)
    logger.info(f"Launching service for {h:02d}h:{m:02d}m:{s:02d}s")

    site = config.client.Site.objects.get(id=config.settings.site_id)
    update_site_from_config(site, config.settings)

    services = config.build_services()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    for service in services:
        logger.info(f"Starting service: {service.__class__.__name__}")
        service.start()

    while not EXIT_FLAG and (time.time() - start_time) < run_time_sec:
        logger.debug("Service sleeping 2...")
        time.sleep(2)

    logger.info("Terminating services...")
    for service in services:
        service.terminate()

    logger.info("Waiting for service processes to join")
    for service in services:
        service.join()

    logger.info("Balsam service: exit main loop")


if __name__ == "__main__":
    multiprocessing.set_start_method("fork", force=True)
    config = SiteConfig()
    run_time_sec = int(config.client.expires_in.total_seconds() - 60)
    with PIDFile(config.site_path):
        main(config, run_time_sec)
    logger.info(f"Balsam service [pid {os.getpid()}] goodbye: exited PIDFile context manager.")
