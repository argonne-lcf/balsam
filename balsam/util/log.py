import logging
import logging.handlers
import sys
import socket
import time
import threading

__version__ = "0.1"

balsam_root_logger = logging.getLogger("balsam")
balsam_root_logger.setLevel(logging.DEBUG)


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
            capacity, flushLevel=flushLevel, target=target, flushOnClose=flushOnClose,
        )
        self.flush_period = flush_period
        self.last_flush = 0
        self.flushLevel = flushLevel
        self.target = target
        self.capacity = capacity
        self.flushOnClose = flushOnClose
        self._flushing_thread = None
        self._schedule_flush()

    def _schedule_flush(self):
        self.flush()
        self._flushing_thread = threading.Timer(
            interval=self.flush_period, function=self._schedule_flush,
        )
        self._flushing_thread.start()

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


def config_logging(filename, level, format, datefmt, buffer_num_records, flush_period):
    level = getattr(logging, level, logging.DEBUG)

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
    balsam_root_logger.addHandler(mem_handler)
    balsam_root_logger.info(f"Logging on {socket.gethostname()}")
    sys.excepthook = log_uncaught_exceptions


def log_uncaught_exceptions(exctype, value, tb):
    balsam_root_logger.error(
        f"Uncaught Exception {exctype}: {value}", exc_info=(exctype, value, tb)
    )
    for handler in balsam_root_logger.handlers:
        handler.flush()
