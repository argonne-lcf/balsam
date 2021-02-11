import logging
import multiprocessing
import os
import signal
import socket
import time
from pathlib import Path
from typing import Any

from balsam.config import SiteConfig

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


def main(config: SiteConfig, run_time_sec: int) -> None:
    start_time = time.time()
    config.enable_logging(basename="service")
    m, s = divmod(run_time_sec, 60)
    h, m = divmod(m, 60)
    logger.info(f"Launching service for {h:02d}h:{m:02d}m:{s:02d}s")

    config.update_site_from_config()

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
