import logging
import multiprocessing

import click

from balsam.site.launcher._serial_mode_master import master_main
from balsam.site.launcher._serial_mode_worker import worker_main

logger = logging.getLogger("balsam.site.launcher")


@click.command()
@click.option("--wall-time-min", type=int)
@click.option("--master-address")
@click.option("--run-master", is_flag=True, default=False)
@click.option("--log-filename")
@click.option("--num-workers", type=int)
@click.option("--filter-tags")
def entry_point(
    wall_time_min: int, master_address: str, run_master: bool, log_filename: str, num_workers: int, filter_tags: str
) -> None:
    master_host, master_port = master_address.split(":")
    if run_master:
        logger.info("Running serial mode master at", master_address)
        master_main(wall_time_min, int(master_port), log_filename, num_workers, filter_tags)
    else:
        logger.info("Running serial mode worker")
        worker_main(master_host, int(master_port), log_filename)


if __name__ == "__main__":
    multiprocessing.set_start_method("fork", force=True)
    entry_point()
