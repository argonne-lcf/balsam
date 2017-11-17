import subprocess
import sys
import shlex
import os
import logging
import time

from django.conf import settings
from balsam.schedulers import exceptions, jobstates
from common import run_subprocess

logger = logging.getLogger(__name__)

class SchedulerException(Exception): pass

def get_environ():
    SchedulerEnv = namedtuple('SchedulerEnv', ['id', 'num_nodes', 'partition'])
    try:
        cobalt_id = os.environ['COBALT_JOBID']
        num_nodes = int(os.environ['COBALT_PARTSIZE'])
        partition = os.environ['COBALT_PARTNAME']
    except KeyError:
        raise SchedulerException("Can't read COBALT_JOBID. Are you really on MOM node?")
    return SchedulerEnv(cobalt_id, num_nodes, partition)


def submit(job, cmd):
    ''' should submit a job to the queue and raise a pre-defined sheduler exception if something fails'''
    logger.info("Submitting Cobalt job: %d", job.id)
    logger.debug("Submitting command: " + cmd)

    command = '%s -A %s -q %s -n %d -t %d --cwd %s %s' % (settings.BALSAM_SCHEDULER_SUBMIT_EXE,
                                                          job.project,
                                                          job.queue,
                                                          job.num_nodes,
                                                          job.wall_time_minutes,
                                                          job.working_directory,
                                                          cmd)
    logger.debug('CobaltScheduler command = %s', command)
    if settings.BALSAM_SUBMIT_JOBS:
        try:
            output = run_subprocess.run_subprocess(command)
            output = output.strip()
            try:
                scheduler_id = int(output)
            except ValueError:
                scheduler_id = int(output.split()[-1])
            logger.debug('CobaltScheduler job (pk=' + str(job.pk) +
                         ') submitted to scheduler as job ' + str(output))
            job.scheduler_id = scheduler_id
        except run_subprocess.SubprocessNonzeroReturnCode as e:
            raise exceptions.SubmitNonZeroReturnCode(
                'CobaltScheduler submit command returned non-zero value. command = "' +
                command +
                '", exception: ' +
                str(e))
        except run_subprocess.SubprocessFailed as e:
            raise exceptions.SubmitSubprocessFailed(
                'CobaltScheduler subprocess to run commit command failed with exception: ' + str(e))
    else:
        raise exceptions.JobSubmissionDisabled(
            'CobaltScheduler Job submission disabled')

    logger.debug('CobaltScheduler Job submission complete')

def get_job_status(scheduler_id):
    class NoQStatInformation(SchedulerException): pass

    qstat = QStat(scheduler_id)
    if not qstat.qstat_info:
        raise NoQStatInformation(f"There was no qstat output for scheduler ID {scheduler_id}")

    info = qstat.qstat_info
    for attr in info:
        if 'time' in attr:
            time = time.strptime(info[attr], '%H:%M:%S')
            time_sec = time.hour*3600 + time.min*60 + time.sec
            info[attr+'_sec'] = time_sec
    return info

class QStat:
    QSTAT_ATTRS = "JobID Nodes WallTime State".split()
    QSTAT_EXE = settings.BALSAM_SCHEDULER_STATUS_EXE

    def __init__(self, scheduler_id):
        qstat_cmd = f"{QSTAT_EXE} {scheduler_id}"

        try:
            os.environ['QSTAT_HEADER'] = ':'.join(QSTAT_ATTRS)
            p = subprocess.Popen( shlex.split(qstat_cmd),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
        except BaseException:
            logger.exception('received exception while trying to run qstat: ' + str(sys.exc_info()[1]))
            raise

        stdout, stderr = p.communicate()
        stdout = stdout.decode('utf-8')
        stderr = stderr.decode('utf-8')
        logger.debug(' qstat ouput: \n' + stdout)
        if p.returncode != 0:
            logger.exception('return code for qstat is non-zero. stdout = \n' +
                stdout + '\n stderr = \n' + stderr)

        try:
            qstat_fields = stdout.split('\n')[2].split()
            self.qstat_info = {attr.lower() : qstat_fields[i] for (i,attr) in
                               enumerate(QSTAT_ATTRS)}
        except:
            self.qstat_info = {}
