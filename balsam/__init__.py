from datetime import datetime
import time
import logging
import logging.handlers
import os
import sys
import setproctitle

try:
    import django
    from django.db import OperationalError
    from django.conf import settings
except ImportError:
    print("Warning: Django is not installed")

__version__ = "0.5.0"


class PeriodicMemoryHandler(logging.handlers.MemoryHandler):
    last_flush = 0
    flush_period = 10

    def flush(self):
        super().flush()
        self.last_flush = time.time()

    def shouldFlush(self, record):
        """
        Check for buffer full or a record at the flushLevel or higher.
        """
        return (len(self.buffer) >= self.capacity) or \
                (record.levelno >= self.flushLevel) or \
                (time.time() - self.last_flush > self.flush_period)


def setup():
    if not settings.configured:
        os.environ['DJANGO_SETTINGS_MODULE'] = 'balsam.django_config.settings'
        django.setup()


_logger = logging.getLogger()
_logger.setLevel(logging.DEBUG)
_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)

# import and use this module early in program lifetime to avoid issues with module init
setproctitle.getproctitle()


def config_logging(basename, filename=None, buffer_capacity=None):
    import multiprocessing_logging
    if filename is None:
        timestamp = datetime.now().strftime('%Y-%m-%d_%H%M%S')
        fname = f'{basename}_{timestamp}.log'
    else:
        fname = filename

    HANDLER_FILE = os.path.join(settings.LOGGING_DIRECTORY, fname)
    formatter = logging.Formatter(
        '%(asctime)s|%(process)d|%(levelname)8s|%(name)s:%(lineno)s] %(message)s',
        datefmt="%d-%b-%Y %H:%M:%S"
    )
    handler = logging.FileHandler(filename=HANDLER_FILE)
    level = getattr(logging, settings.LOG_HANDLER_LEVEL)
    handler.setLevel(level)
    handler.setFormatter(formatter)

    if not buffer_capacity:
        _logger.addHandler(handler)
    else:
        mem_handler = PeriodicMemoryHandler(capacity=buffer_capacity, target=handler)
        mem_handler.setLevel(level)
        mem_handler.setFormatter(formatter)
        _logger.addHandler(mem_handler)
    multiprocessing_logging.install_mp_handler()

    return fname


def log_uncaught_exceptions(exctype, value, tb):
    if isinstance(value, OperationalError) and not settings.DATABASES['default']['PORT']:
        _logger.error("Balsam OperationalError: No DB is currently active")
        _logger.error("Please use `source balsamactivate` to activate a Balsam DB")
        _logger.error("Use `balsam which --list` for a listing of known DB names")
    else:
        _logger.error(f"Uncaught Exception {exctype}: {value}",exc_info=(exctype,value,tb))
        for handler in _logger.handlers: handler.flush()


sys.excepthook = log_uncaught_exceptions
__all__ = ['config_logging', 'settings', 'setup']
