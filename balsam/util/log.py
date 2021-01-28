import logging
import logging.handlers
import os
import socket
import sys
import textwrap
import time

import multiprocessing_logging


class PeriodicMemoryHandler(logging.handlers.MemoryHandler):
    """
    Buffer logs in memory; flush when:
        - urgency exceeds flushLevel (see errors right away)
        - logs exceed buffer capacity
        - flush_period seconds have elapsed
    A background thread is responsible for periodically flushing when the
    buffer is sitting idle.
    """

    def __init__(
        self,
        capacity,
        flushLevel=logging.ERROR,
        target=None,
        flushOnClose=True,
        flush_period=30,
    ):
        super().__init__(
            capacity,
            flushLevel=flushLevel,
            target=target,
            flushOnClose=flushOnClose,
        )
        self.flush_period = flush_period
        self.last_flush = 0
        self.flushLevel = flushLevel
        self.target = target
        self.capacity = capacity
        self.flushOnClose = flushOnClose

    def flush(self):
        super().flush()
        self.last_flush = time.time()

    def shouldFlush(self, record):
        """
        Check for buffer full or a record at the flushLevel or higher.
        """
        return (
            (len(self.buffer) >= self.capacity)
            or (record.levelno >= self.flushLevel)
            or (time.time() - self.last_flush > self.flush_period)
        )


def validate_log_level(level):
    if isinstance(level, str):
        return getattr(logging, level, logging.DEBUG)
    return min(50, max(0, level))


def config_root_logger(level=None):
    if level is None:
        level = validate_log_level(os.environ.get("BALSAM_LOG_LEVEL", "DEBUG"))
    logger = logging.getLogger("balsam")
    logger.setLevel(level)
    if logger.hasHandlers():
        logger.handlers.clear()

    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(levelname)s|%(name)s:%(lineno)s] %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger, handler


def config_file_logging(
    filename,
    level,
    format,
    datefmt,
    buffer_num_records,
    flush_period,
):
    level = validate_log_level(level)
    root_logger, _ = config_root_logger(level)

    file_handler = logging.FileHandler(filename=filename)
    mem_handler = PeriodicMemoryHandler(
        capacity=buffer_num_records,
        flushLevel=logging.ERROR,
        target=file_handler,
        flush_period=flush_period,
    )

    formatter = logging.Formatter(format, datefmt=datefmt)
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    mem_handler.setLevel(level)
    mem_handler.setFormatter(formatter)
    if root_logger.hasHandlers():
        root_logger.handlers.clear()
    root_logger.addHandler(mem_handler)
    multiprocessing_logging.install_mp_handler(logger=root_logger)
    root_logger.info(f"Configured logging on {socket.gethostname()}")
    sys.excepthook = log_uncaught_exceptions


def log_uncaught_exceptions(exctype, value, tb):
    root_logger = logging.getLogger("balsam")
    root_logger.error(f"Uncaught Exception {exctype}: {value}", exc_info=(exctype, value, tb))


def banner(message, color="HEADER"):
    bcolors = {
        "HEADER": "\033[95m",
        "OKBLUE": "\033[94m",
        "OKGREEN": "\033[92m",
        "WARNING": "\033[93m",
        "FAIL": "\033[91m",
        "ENDC": "\033[0m",
        "BOLD": "\033[1m",
        "UNDERLINE": "\033[4m",
    }
    message = "\n".join(line.strip() for line in message.split("\n"))
    lines = textwrap.wrap(message, width=80)
    width = max(len(line) for line in lines) + 4
    header = "*" * width
    msg = f" {header}\n"
    for line in lines:
        msg += "   " + line + "\n"
    msg += f" {header}"
    if sys.stdout.isatty():
        print(bcolors.get(color), msg, bcolors["ENDC"], sep="")
    else:
        print(msg)
