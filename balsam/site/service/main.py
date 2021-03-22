import logging
import multiprocessing
import os
import socket
import time
from pathlib import Path
from typing import Any

from balsam.config import SiteConfig
from balsam.util import SigHandler

logger = logging.getLogger("balsam.site.service.main")


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
    sig_handler = SigHandler()
    config.enable_logging(basename="service")
    m, s = divmod(run_time_sec, 60)
    h, m = divmod(m, 60)
    logger.info(f"Launching service for {h:02d}h:{m:02d}m:{s:02d}s")

    config.update_site_from_config()

    services = config.build_services()

    for service in services:
        attrs = {k: v for k, v in service.__dict__.items() if not k.startswith("_")}
        logger.info(f"Starting service: {service.__class__.__name__}")
        logger.info(str(attrs))
        service.start()

    while not sig_handler.wait_until_exit(timeout=60):
        elapsed = time.time() - start_time
        if elapsed > run_time_sec:
            logger.info("Exceeded max service runtime")
            break
        remaining = int(run_time_sec - elapsed)
        m, s = divmod(remaining, 60)
        if (m, s) >= (60, 0):
            h, m = divmod(m, 60)
            logger.info(f"{h:02d}h:{m:02d}m remaining")
        else:
            logger.info(f"{m:02d}m:{s:02d}s remaining")

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
    run_time_sec = int(config.client.expires_in.total_seconds() - 120)
    if run_time_sec < 10:
        raise RuntimeError(
            f"Client authorization will expire in {run_time_sec+120} seconds. Please refresh auth with `balsam login`."
        )

    with PIDFile(config.site_path):
        main(config, run_time_sec)
    logger.info(f"Balsam service [pid {os.getpid()}] goodbye: exited PIDFile context manager.")
