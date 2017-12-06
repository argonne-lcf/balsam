import subprocess
import sys
import shlex
import os
from datetime import datetime
from collections import namedtuple

from django.conf import settings
from balsam.schedulers.exceptions import * 
from balsam.schedulers import Scheduler
from common import run_subprocess

import logging
logger = logging.getLogger(__name__)

def new_scheduler():
    return CobaltScheduler()

class CobaltScheduler(Scheduler.Scheduler):
    SCHEDULER_VARIABLES = {
        'id' : 'COBALT_JOBID',
        'num_workers'  : 'COBALT_PARTSIZE',
        'workers_str'  : 'COBALT_PARTNAME',
        'workers_file' : 'COBALT_NODEFILE',
    }
    JOBSTATUS_VARIABLES = {
        'id' : 'JobID',
        'time_remaining' : 'TimeRemaining',
        'state' : 'State',
    }
    GENERIC_NAME_MAP = {v:k for k,v in JOBSTATUS_VARIABLES.items()}

    def _make_submit_cmd(self, job, cmd):
        exe = settings.BALSAM_SCHEDULER_SUBMIT_EXE # qsub
        return (f"{exe} -A {job.project} -q {job.queue} -n {job.num_nodes} "
           f"-t {job.wall_time_minutes} --cwd {job.working_directory} {cmd}")

    def _post_submit(self, job, cmd, submit_output):
        '''Return a SubmissionRecord: contains scheduler ID'''
        try: scheduler_id = int(output)
        except ValueError: scheduler_id = int(output.split()[-1])
        logger.debug(f'job {job.cute_id} submitted as Cobalt job {scheduler_id}')
        return Scheduler.SubmissionRecord(scheduler_id=scheduler_id)

    def get_status(self, scheduler_id, jobstatus_vars=None):
        if jobstatus_vars is None: 
            jobstatus_vars = self.JOBSTATUS_VARIABLES.values()
        else:
            jobstatus_vars = [self.JOBSTATUS_VARIABLES[a] for a in jobstatus_vars]

        logger.debug(f"Cobalt ID {scheduler_id} get_status:")
        info = qstat(scheduler_id, jobstatus_vars)
        info = {self.GENERIC_NAME_MAP[k] : v for k,v in info.items()}

        time_attrs_seconds = {k+"_sec" : datetime.strptime(v, '%H:%M:%S')
                              for k,v in info.items() if 'time' in k}
        for k,time in time_attrs_seconds.items():
            time_attrs_seconds[k] = time.hour*3600 + time.minute*60 + time.second
        info.update(time_attrs_seconds)
        logger.debug(str(info))
        return info

def qstat(scheduler_id, attrs):
    exe = settings.BALSAM_SCHEDULER_STATUS_EXE
    qstat_cmd = f"{exe} {scheduler_id}"
    os.environ['QSTAT_HEADER'] = ':'.join(attrs)

    try:
        p = subprocess.Popen(shlex.split(qstat_cmd),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
    except OSError:
        raise JobStatusFailed(f"could not execute {qstat_cmd}")

    stdout, _ = p.communicate()
    stdout = stdout.decode('utf-8')
    if p.returncode != 0:
        logger.exception('return code for qstat is non-zero:\n'+stdout)
        raise NoQStatInformation("qstat nonzero return code: this might signal job is done")
    try:
        logger.debug('parsing qstat ouput: \n' + stdout)
        qstat_fields = stdout.split('\n')[2].split()
        qstat_info = {attr : qstat_fields[i] for (i,attr) in
                           enumerate(attrs)}
    except:
        raise NoQStatInformation
    else:
        return qstat_info
