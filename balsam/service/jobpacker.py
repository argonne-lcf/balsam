from balsam.service import models
BalsamJob = models.BalsamJob
QueuedLaunch = models.QueuedLaunch

_pack_jobs = dummy_pack

def create_qlaunch(jobs, queues):
    qlaunch, to_launch = _pack_jobs(jobs, queues)
    if qlaunch:
        qlaunch.save()
        num = BalsamJob.objects.filter(pk__in=to_launch).update(queued_launch=qlaunch)
        logger.info(f'Scheduled {num} jobs in {qlaunch}')
        return qlaunch
    else:
        return None

def dummy_pack(jobs, queues):
    '''Input: jobs (queryset of scheduleable jobs), queues(states of all queued
    jobs); Return: a qlaunch object (from which launcher qsub can be generated),
    and list/queryset of jobs scheduled for that launch'''
    if queues.num_queued >= 1: return None, 0
    qname = queues.names[0]
    qlaunch = QueuedLaunch(queue=qname,
                           nodes=4,
                           job_mode='mpi',
                           time_minutes=10)
    jobs = jobs.all()
    return qlaunch, jobs
