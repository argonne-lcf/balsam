from pathlib import Path
import logging
import signal
from balsam.config import SiteConfig
import os
import socket
import sys
import time

logger = logging.getLogger(__name__)

EXIT_FLAG = False


def handler(signum, stack):
    global EXIT_FLAG
    EXIT_FLAG = True


def check_and_set_pidfile(site_path):
    pidfile: Path = site_path.joinpath("balsam-service.pid")
    if pidfile.exists():
        sys.stderr.write(
            f"{pidfile} already exists: will not start Balsam service because it's already running"
        )
        sys.stderr.write(
            "To restart, first stop the Balsam service with `balsam service stop`"
        )
        sys.exit(1)
    else:
        with open(pidfile, "w") as fp:
            fp.write(f"{socket.gethostname()} {os.getpid()}")


def main():
    config = SiteConfig()
    check_and_set_pidfile(config.site_path)
    config.enable_logging(basename="service")

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
    main()
