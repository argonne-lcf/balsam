from balsam.core import models
import logging
logger = logging.getLogger(__name__)

JOB_PAD_MINUTES = 5
BalsamJob = models.BalsamJob
QueuedLaunch = models.QueuedLaunch


def create_qlaunch(queues):
    jobs = ready_query()
    qlaunch, to_launch = _pack_jobs(jobs, queues)
    if qlaunch and to_launch.exists():
        # qlaunch.wall_minuntes += JOB_PAD_MINUTES
        qlaunch.save()
        qlaunch.refresh_from_db()
        num = to_launch.update(queued_launch=qlaunch)
        logger.info(f'Scheduled {num} jobs in {qlaunch}')
        return qlaunch
    else:
        return None

    jobs = jobs.all()
    return qlaunch, jobs


def ready_query():
    return BalsamJob.objects.filter(
        lock='',
        queued_launch__isnull=True,
        state__in=['PREPROCESSED', 'RESTART_READY', 'AWAITING_PARENTS']
    )


def dummy_pack(jobs, queues):
    '''Input: jobs (queryset of scheduleable jobs), queues(states of all queued
    jobs); Return: a qlaunch object (from which launcher qsub can be generated),
    and list/queryset of jobs scheduled for that launch'''
    if not queues:
        return None
    # qname = list(queues.keys())[0]
    qlaunch = QueuedLaunch(queue='default',
                           nodes=256,
                           job_mode='mpi',
                           prescheduled_only=False,
                           wall_minutes=360)
    return qlaunch, jobs.all()


_pack_jobs = dummy_pack
