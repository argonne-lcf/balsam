pack_method = dummy_pack

def pack(queues):
    qlaunch, balsamjobs = pack_method(queues)
    qlaunch.save()
    jobs = BalsamJob.objects.filter(pk__in=balsamjobs)
    jobs.update(queued_launch=qlaunch)

def dummy_pack(queues):
    count = models.QueuedLaunch.objects.count()
    if count >= 1: return
    qname = queues.names[0]
    qlaunch = QueuedLaunch(queue=qname,
                           nodes=4,
                           job_mode='mpi',
                           time_minutes=10)
    return qlaunch, BalsamJob.objects.all()
