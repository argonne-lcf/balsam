from datetime import datetime
import logging
import os
import sys

import django
from django.db import OperationalError
from django.conf import settings

if not settings.configured:
    os.environ['DJANGO_SETTINGS_MODULE'] = 'balsam.django_config.settings'
    django.setup()

_logger = logging.getLogger()
_logger.setLevel(logging.DEBUG)
_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)

def config_logging(basename):
    timestamp = datetime.now().strftime('%Y-%m-%d_%H%M%S')
    fname = f'{basename}_{timestamp}.log'
    HANDLER_FILE = os.path.join(settings.LOGGING_DIRECTORY, fname)
        
    formatter=logging.Formatter(
            '%(asctime)s|%(process)d|%(levelname)8s|%(name)s:%(lineno)s] %(message)s', 
            datefmt="%d-%b-%Y %H:%M:%S"
            )
    handler = logging.handlers.RotatingFileHandler(filename=HANDLER_FILE,
            maxBytes=settings.LOG_FILE_SIZE_LIMIT,
            backupCount=settings.LOG_BACKUP_COUNT)
    level = getattr(logging, settings.LOG_HANDLER_LEVEL)
    handler.setLevel(level)
    handler.setFormatter(formatter)
    _logger.addHandler(handler)

def log_uncaught_exceptions(exctype, value, tb):
    _logger.error(f"Uncaught Exception {exctype}: {value}",exc_info=(exctype,value,tb))
    for handler in _logger.handlers: handler.flush()

    if isinstance(value, OperationalError):
        db_path = os.environ.get('BALSAM_DB_PATH')
        if not settings.DATABASES['default']['PORT']:
            print("Balsam OperationalError: No DB is currently active")
            print("Please use `source balsamactivate` to activate a Balsam DB")
            print("Use `balsam which --list` for a listing of known DB names")
        else:
            print("Failed to reach the Balsam DB server at", db_path, f"(use 'balsam log' for detailed traceback)")
    else:
        console = logging.getLogger('console')
        console.error(f"Uncaught Exception",exc_info=(exctype,value,tb))
        [h.flush() for h in console.handlers]

sys.excepthook = log_uncaught_exceptions
__all__ = ['config_logging', 'settings']
