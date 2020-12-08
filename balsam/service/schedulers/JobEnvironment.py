import os
import time
from socket import gethostname
from balsam.service.schedulers.exceptions import (
    NoQStatInformation)

import logging
logger = logging.getLogger(__name__)


class JobEnvironment:
    # TODO(KGF): change keys to lowercase to be consistent with class names in
    # mpi_commands.py. However, will req changes to worker.py::setup_SLURM(self)
    RECOGNIZED_HOSTS = {
        'BGQ': 'vesta cetus mira'.split(),
        'THETA': 'theta'.split(),
        'COOLEY': 'cooley cc'.split(),
        'SLURM': 'bebop blues lcrc'.split(),
    }

    def __init__(self, scheduler):
        self.scheduler_vars = scheduler.SCHEDULER_VARIABLES
        self.pid = os.getpid()
        self.hostname = gethostname()
        self.current_scheduler_id = None
        self.num_workers = 1
        self.workers_str = None
        self.workers_file = None
        self.remaining_seconds = float("inf")
        self.get_env()
        try:
            info = scheduler.get_status(self.current_scheduler_id)
            self.remaining_seconds = info['time_remaining_sec']
        except (NoQStatInformation, TypeError, KeyError):
            pass
        self._last_check_seconds = time.time()

    def get_env(self):
        '''Check for environment variables (e.g. COBALT_JOBID) indicating
        currently inside a scheduled job'''
        for generic_name, specific_var in self.scheduler_vars.items():
            value = os.environ.get(specific_var, None)
            if value is not None and value.startswith('num'):
                value = int(value)
            setattr(self, generic_name, value)

        if self.current_scheduler_id:
            self.current_scheduler_id = int(self.current_scheduler_id)
            logger.debug(f"Detected scheduler ID {self.current_scheduler_id}")

    def remaining_time_seconds(self):
        '''Either counts down from RemainingTime obtained from scheduler, or infinity'''
        now = time.time()
        elapsed_time = now - self._last_check_seconds
        self.remaining_seconds -= elapsed_time
        self._last_check_seconds = now
        return self.remaining_seconds
