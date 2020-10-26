from pathlib import Path
import logging
import signal
import os
import socket
import time

from balsam.config import SiteConfig, Settings
from balsam.api import Site

logger = logging.getLogger(__name__)

EXIT_FLAG = False


def handler(signum, stack):
    global EXIT_FLAG
    EXIT_FLAG = True


class PIDFile:
    def __init__(self, site_path: Path):
        self.path: Path = site_path.joinpath("balsam-service.pid")

    def __enter__(self):
        if self.path.exists():
            raise RuntimeError(
                f"{self.path} already exists: will not start Balsam service because it's already running. "
                f"To restart, first stop the Balsam service with `balsam service stop`"
            )
        with open(self.path, "w") as fp:
            fp.write(f"{socket.gethostname()} {os.getpid()}")

    def __exit__(self, exc_type, exc_value, traceback):
        self.path.unlink()


def update_site_from_config(site: Site, settings: Settings):
    old_dict = site.display_dict()
    site.allowed_projects = settings.scheduler.allowed_projects
    site.allowed_queues = settings.scheduler.allowed_queues
    site.optional_batch_job_params = settings.scheduler.optional_batch_job_params
    site.transfer_locations = settings.transfers.transfer_locations
    site.globus_endpoint_id = settings.transfers.globus_endpoint_id

    new_dict = site.display_dict()
    diff = {
        k: (old_dict[k], new_dict[k]) for k in old_dict if old_dict[k] != new_dict[k]
    }
    if diff:
        site.save()
        diff_str = "\n".join(f"{k}={diff[k][0]} --> {diff[k][1]}" for k in diff)
        logger.info(f"Updated Site parameters:\n{diff_str}")


def main(config: SiteConfig):
    config.enable_logging(basename="service")

    site = Site.objects.get(id=config.settings.site_id)
    update_site_from_config(site, config.settings)

    services = config.build_services()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    for service in services:
        logger.info(f"Starting service: {service.__name__}")
        service.start()

    while not EXIT_FLAG:
        time.sleep(1)

    logger.info(f"Signal: terminating services")

    for service in services:
        service.terminate()

    logger.info(f"Waiting for service processes to join")

    for service in services:
        service.join()

    logger.info(f"Balsam service: exit graceful")


if __name__ == "__main__":
    config = SiteConfig()
    with PIDFile(config.site_path):
        main(config)
