import subprocess
import sys
import shlex
import os
import time
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
        'num_workers' : 'COBALT_PARTSIZE',
        'workers_str' : 'COBALT_PARTNAME',
    }
    JOBSTATUS_VARIABLES = {
        'id' : 'JobID',
        'time_remaining' : 'TimeRemaining',
        'state' : 'State',
    }

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
            jobstatus_vars = JOBSTATUS_VARIABLES.values()
        else:
            jobstatus_vars = [JOBSTATUS_VARIABLES[a] for a in jobstatus_vars]
        info = qstat(scheduler_id, attrs)
        for attr in info:
            if 'time' in attr:
                time = time.strptime(info[attr], '%H:%M:%S')
                time_sec = time.hour*3600 + time.min*60 + time.sec
                info[attr+'_sec'] = time_sec
        return info

def qstat(scheduler_id, attrs):
    exe = settings.BALSAM_SCHEDULER_STATUS_EXE
    qstat_cmd = f"{exe} {scheduler_id}"
    os.environ['QSTAT_HEADER'] = ':'.join(attrs)

    p = subprocess.Popen(shlex.split(qstat_cmd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)

    stdout, stderr = p.communicate()
    stdout = stdout.decode('utf-8')
    stderr = stderr.decode('utf-8')
    logger.debug('qstat ouput: \n' + stdout)
    if p.returncode != 0:
        logger.error('return code for qstat is non-zero. stdout = \n' +
            stdout + '\n stderr = \n' + stderr)
    try:
        qstat_fields = stdout.split('\n')[2].split()
        qstat_info = {attr : qstat_fields[i] for (i,attr) in
                           enumerate(attrs)}
    except:
        raise NoQStatInformation
    else:
        return qstat_info
