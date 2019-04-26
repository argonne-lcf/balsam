from balsam.core import models
from django.conf import settings
from django.db.models import F, Sum, FloatField
from django.db.models.functions import Cast
import logging
logger = logging.getLogger(__name__)

JOB_PAD_MINUTES = 5
BalsamJob = models.BalsamJob
QueuedLaunch = models.QueuedLaunch

def create_qlaunch(queues):
    jobs = ready_query()
    qlaunch, to_launch = _pack_jobs(jobs, queues)
    if qlaunch and to_launch.exists():
        #qlaunch.wall_minuntes += JOB_PAD_MINUTES
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
        state__in=['PREPROCESSED', 'RESTART_READY']
    )

def serial_node_count():
    jobs = ready_query()
    serial_jobs = jobs.filter(num_nodes=1, ranks_per_node=1)
    serial_jobs = serial_jobs.annotate(
        nodes=1.0/Cast('node_packing_count', FloatField())
        +Cast('coschedule_num_nodes', FloatField())
    )
    return serial_jobs.aggregate(total=Sum('nodes'))['total']

def box_pack(jobs, queues):
    import _packer
    Rect = _packer.Rect
    if serial_node_count() > SERIAL_PACK_THRESHOLD:
        jobs = ready_query.all()
    job_rects = [Rect(*job) for job in jobs.values_list(
                 ['num_nodes', ''])]
    for q in queues:
        pass

def dummy_pack(jobs, queues):
    '''Input: jobs (queryset of scheduleable jobs), queues(states of all queued
    jobs); Return: a qlaunch object (from which launcher qsub can be generated),
    and list/queryset of jobs scheduled for that launch'''
    if not queues: return None
    qname = list(queues.keys())[0]
    qlaunch = QueuedLaunch(queue=qname,
                           nodes=8,
                           job_mode='mpi',
                           prescheduled_only=False,
                           wall_minutes=60)
    return qlaunch, jobs.all()

_pack_jobs = dummy_pack
