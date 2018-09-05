from balsam.service import models
from django.conf import settings
import logging

logger = logging.getLogger(__name__)

BalsamJob = models.BalsamJob
QueuedLaunch = models.QueuedLaunch

def create_qlaunch(jobs, queues):
    qlaunch, to_launch = _pack_jobs(jobs, queues)
    if qlaunch:
        qlaunch.save()
        qlaunch.refresh_from_db()
        num = to_launch.update(queued_launch=qlaunch)
        logger.info(f'Scheduled {num} jobs in {qlaunch}')
        return qlaunch
    else:
        return None

def dummy_pack(jobs, queues):
    '''Input: jobs (queryset of scheduleable jobs), queues(states of all queued
    jobs); Return: a qlaunch object (from which launcher qsub can be generated),
    and list/queryset of jobs scheduled for that launch'''
    if not queues: return None
    qname = list(queues.keys())[0]
    qlaunch = QueuedLaunch(queue=qname,
                           nodes=3,
                           job_mode='serial',
                           wall_minutes=12)
    jobs = jobs.all()
    return qlaunch, jobs

def box_pack(jobs, queues):
    # query parents and states
    # tag jobs that can be placed now (no parents or parents finished)
    # tag jobs with pending dependencies
    # first pass: first-fit decreasing: filter placeable jobs only
    # stack jobs in first column only
    # fix  nodes, determine max walltime
    # if any jobs longer than maxwalltime; filter out, then re-run
    for q in queues:
        pass

_pack_jobs = dummy_pack
