import os
import subprocess
import sys
import time
from socket import gethostname

import logging
logger = logging.getLogger(__name__)

class JobEnvironment:
    RECOGNIZED_HOSTS = {
        'BGQ'    : 'vesta cetus mira'.split(),
        'THETA'   : 'theta'.split(),
        'COOLEY' : 'cooley cc'.split()
    }

    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.pid = os.getpid()
        self.hostname = gethostname()
        self.host_type = 'DEFAULT'
        
        self.current_scheduler_id = None
        self.num_workers = None
        self.workers_str = None
        self.workers_file = None
        self.remaining_seconds = None
        self.get_env()
        self.remaining_time_seconds()

        for host_type, known_names in self.RECOGNIZED_HOSTS.items():
            if any(self.hostname.find(name) >= 0 for name in known_names):
                self.host_type = host_type
        logger.debug(f"Recognized host_type: {self.host_type}")

    def get_env(self):
        '''Check for environment variables (e.g. COBALT_JOBID) indicating 
        currently inside a scheduled job'''
        for generic_name, specific_var in self.scheduler.SCHEDULER_VARIABLES.items():
            value = os.environ.get(specific_var, None)
            if value is not None and value.startswith('num'): value = int(value)
            setattr(self, generic_name, value)

        if self.current_scheduler_id:
            self.current_scheduler_id = int(self.current_scheduler_id)
            logger.debug(f"Detected scheduler ID {self.current_scheduler_id}")
        else:
            logger.debug(f"Did not detect a {self.scheduler} ID")

    def _read_remaining_seconds(self):
        if self.current_scheduler_id is None:
            return float("inf")
        try:
            info = self.scheduler.get_status(self.current_scheduler_id)
        except JobStatusFailed:
            return float("inf")
        else:
            return info['time_remaining_sec']

    def remaining_time_seconds(self):
        if self.remaining_seconds is None:
            self.remaining_seconds = self._read_remaining_seconds()
            self.last_check_seconds = time.time()
        now = time.time()
        elapsed_time = now - self.last_check_seconds
        self.remaining_seconds -= elapsed_time
        self.last_check_seconds = now
        return self.remaining_seconds

def get_balsam_env():
    conda_env = os.environ.get('CONDA_PREFIX')
    if conda_env:
        activate_path = os.path.join(conda_env, 'bin', 'activate')
        conda_path = conda_env
        python_path = None
    else:
        activate_path = None
        conda_path = None
        python_path = os.path.dirname(sys.executable)

    balsam_lib = os.path.dirname(find_spec('balsam').origin)
    p = subprocess.run('which balsam', shell=True, stdout=subprocess.PIPE, encoding='utf-8')
    balsam_bin = os.path.dirname(p.stdout)
    site_top = os.path.dirname(os.path.dirname(find_spec('django').origin))
    balsam_db_path = os.environ['BALSAM_DB_PATH']
    return dict(conda_path=conda_path,
                activate_path=activate_path,
                python_path=python_path,
                balsam_lib=balsam_lib,
                balsam_bin=balsam_bin,
                site_top=site_top,
                balsam_db_path=balsam_db_path)
