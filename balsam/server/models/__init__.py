import logging
from .user import User
from .site import Site
from .app import App
from .job import Job, EventLog, JobLock
from .transfer import TransferItem, TransferTask
from .batchjob import BatchJob

logger = logging.getLogger(__name__)
TIME_FMT = '%m-%d-%y %H:%M:%S.%f'

MODELS = {
    'User': User,
    'Site': Site,
    'App': App,
    'Job': Job,
    'EventLog': EventLog,
    'JobLock': JobLock,
    'TransferItem': TransferItem,
    'TransferTask': TransferTask,
    'BatchJob': BatchJob,
}