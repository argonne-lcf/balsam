from django.conf import settings
from importlib import import_module
from balsam.schedulers.exceptions import *
from socket import gethostname
import os
import time

import logging
logger = logging.getLogger(__name__)

class SubmissionRecord:
    '''Extend me: contains any data returned after queue submission'''
    def __init__(self, *, scheduler_id=None):
        self.scheduler_id = scheduler_id

class Scheduler:
    RECOGNIZED_HOSTS = {
        'BGQ'    : 'vesta cetus mira'.split(),
        'CRAY'   : 'theta'.split(),
        'COOLEY' : 'cooley cc'.split()
    }
    SCHEDULER_VARIABLES = {} # mappings defined in subclass
    JOBSTATUS_VARIABLES = {}

    def __init__(self):
        self.hostname = None
        self.host_type = None
        self.pid = None
        self.num_workers = None
        self.workers_str = None
        self.workers_file = None
        self.current_scheduler_id = None
        self.remaining_seconds = None
        self.last_check_seconds = None

        self.pid = os.getpid()
        self.hostname = gethostname()
        for host_type, known_names in self.RECOGNIZED_HOSTS.items():
            if any(self.hostname.find(name) >= 0 for name in known_names):
                self.host_type = host_type
        if self.host_type is None:
            self.host_type = 'DEFAULT'

        logger.debug(f"Recognized host_type: {self.host_type}")
        logger.debug(f"Using scheduler class {self.__class__}")

        try: 
            self.get_env()
        except SchedulerException: 
            logger.debug(f"Did not detect a scheduler ID")
            return
        else:
            logger.debug(f"Detected scheduler ID {self.current_scheduler_id}")

    def get_env(self):
        '''Check for environment variables (e.g. COBALT_JOBID) indicating 
        currently inside a scheduled job'''
        environment = {}
        for generic_name, specific_var in self.SCHEDULER_VARIABLES.items():
            environment[generic_name] = os.environ.get([specific_var], None)

        if environment['id']:
            self.current_scheduler_id = environment['id']
        if environment['num_workers']:
            self.num_workers = int(environment['num_workers'])
        if environment['workers_str']:
            self.workers_str = environment['workers_str']
        if environment['workers_file']:
            self.workers_file = environment['workers_file']

        if not environment['id']:
            raise SchedulerException(f"No ID in environment")
        return environment

    def remaining_time_seconds(self, sched_id=None):
        '''Remaining time from a qstat or internal timer'''
        if sched_id:
            info = self.get_status(sched_id)
            return info['time_remaining_sec']
        elif self.remaining_seconds:
            now = time.time()
            elapsed_time = now - self.last_check_seconds
            self.remaining_seconds -= elapsed_time
            self.last_check_seconds = now
            return self.remaining_seconds

        sched_id = self.current_scheduler_id
        if sched_id is None:
            return float("inf")
        info = self.get_status(sched_id)
        self.remaining_seconds = info['time_remaining_sec']
        self.last_check_seconds = time.time()
        logger.debug(f"{self.remaining_seconds} seconds remaining")
        return self.remaining_seconds

    def submit(self, job, cmd):
        logger.info(f"Submit {job.cute_id} {cmd} ({self.__class__})")
        self._pre_submit(self, job, cmd)
        submit_cmd = self._make_submit_cmd(job, cmd)
        p = subprocess.Popen(submit_cmd, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT)
        stdout, stderr = p.communicate()
        if p.returncode != 0: raise SubmitNonZeroReturnCode
        submissionRecord = self._post_submit(job, cmd, stdout)
        return submissionRecord

    def get_status(self, scheduler_id, jobstatus_vars=['state','time_remaining']):
        raise NotImplementedError

    def _make_submit_cmd(self, job, cmd):
        '''must return the string which is passed to Popen'''
        raise NotImplementedError

    def _pre_submit(self, job, cmd):
        '''optional pre-submit'''
        pass

    def _post_submit(self, job, cmd, submit_output):
        '''Returns SubmissionRecord: contains scheduler ID'''
        pass

scheduler_class = settings.BALSAM_SCHEDULER_CLASS

if scheduler_class:
    _temp = import_module('balsam.schedulers.'+scheduler_class)
    scheduler_main = _temp.new_scheduler()
else:
    scheduler_main = Scheduler()
