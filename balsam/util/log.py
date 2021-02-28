import itertools
import logging
import logging.handlers
import os
import socket
import sys
import threading
import time
from pathlib import Path
from typing import Any, Optional, TextIO, Union

import multiprocessing_logging  # type: ignore

from .sighandler import SigHandler


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
        capacity: int,
        target: logging.Handler,
        flushLevel: int = logging.ERROR,
        flushOnClose: bool = True,
        flush_period: int = 30,
    ) -> None:
        super().__init__(
            capacity,
            flushLevel=flushLevel,
            target=target,
            flushOnClose=flushOnClose,
        )
        self.flush_period = flush_period
        self.last_flush = 0.0
        self.flushLevel = flushLevel
        self.target = target
        self.capacity = capacity
        self.flushOnClose = flushOnClose

    def flush(self) -> None:
        super().flush()
        self.last_flush = time.time()

    def shouldFlush(self, record: logging.LogRecord) -> bool:
        """
        Check for buffer full or a record at the flushLevel or higher.
        """
        return (
            (len(self.buffer) >= self.capacity)
            or (record.levelno >= self.flushLevel)
            or (time.time() - self.last_flush > self.flush_period)
        )


def validate_log_level(level: Union[str, int]) -> int:
    try:
        level = int(level)
    except ValueError:
        level = getattr(logging, str(level), logging.DEBUG)
        return int(level)
    return min(50, max(0, level))


def config_root_logger(level: Union[str, int, None] = None) -> logging.Logger:
    if level is None:
        level = validate_log_level(os.environ.get("BALSAM_LOG_LEVEL", "WARNING"))
    logger = logging.getLogger("balsam")
    logger.setLevel(level)
    if logger.hasHandlers():
        logger.handlers.clear()

    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(levelname)s|%(name)s:%(lineno)s] %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def config_file_logging(
    filename: Union[str, Path],
    level: int,
    format: str,
    datefmt: str,
    buffer_num_records: int,
    flush_period: int,
) -> None:
    level = validate_log_level(level)
    root_logger = config_root_logger(level)

    file_handler = logging.FileHandler(filename=filename)
    mem_handler = PeriodicMemoryHandler(
        capacity=buffer_num_records,
        flushLevel=logging.ERROR,
        target=file_handler,
        flush_period=flush_period,
    )

    formatter = logging.Formatter(format, datefmt=datefmt)
    file_handler.setFormatter(formatter)
    root_logger.handlers.clear()
    root_logger.addHandler(mem_handler)

    # Trap SIGTERM, SIGINT to avoid broken pipes:
    SigHandler()
    multiprocessing_logging.install_mp_handler(logger=root_logger)
    root_logger.info(f"Configured logging on {socket.gethostname()}")
    sys.excepthook = log_uncaught_exceptions


def log_uncaught_exceptions(exctype: Any, value: Any, tb: Any) -> None:
    root_logger = logging.getLogger("balsam")
    root_logger.error(f"Uncaught Exception {exctype}: {value}", exc_info=(exctype, value, tb))


class Spinner(object):
    """
    https://github.com/click-contrib/click-spinner
    """

    spinner_cycle = itertools.cycle(["-", "/", "|", "\\"])

    def __init__(self, msg: str, stream: TextIO = sys.stdout) -> None:
        self.msg = msg
        self.stream = stream
        self.stop_running: Optional[threading.Event] = None
        self.spin_thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self.stream.write(self.msg + "  ")
        if self.stream.isatty():
            self.stop_running = threading.Event()
            self.spin_thread = threading.Thread(target=self.init_spin)
            self.spin_thread.start()

    def stop(self) -> None:
        assert self.stop_running is not None
        if self.spin_thread:
            self.stop_running.set()
            self.spin_thread.join()

    def init_spin(self) -> None:
        assert self.stop_running is not None
        while not self.stop_running.is_set():
            self.stream.write(next(self.spinner_cycle))
            self.stream.flush()
            self.stop_running.wait(0.25)
            self.stream.write("\b")
            self.stream.flush()

    def __enter__(self) -> "Spinner":
        self.start()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.stop()
        return None
