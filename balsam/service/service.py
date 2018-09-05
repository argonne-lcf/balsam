import logging
import os
import sys
import signal
import stat
import time
from socket import gethostname

from django.db.models import Count
from balsam import config_logging, settings
from balsam.service import models, queues, jobpacker
from balsam.service.schedulers import script_template, scheduler
from balsam.scripts.cli import service_subparser
from balsam.launcher import transitions

logger = logging.getLogger('balsam.service.service')
QueuedLaunch = models.QueuedLaunch
BalsamJob = models.BalsamJob
EXIT_FLAG = False

def submit_qlaunch(qlaunch, verbose=False):
    top = settings.SERVICE_PATH
    pk = qlaunch.pk
    script_path = os.path.join(top, f'qlaunch{pk}.sh')
    if os.path.exists(script_path):
        raise ValueError("Job script already rendered for {qlaunch}")
    script = script_template.render(qlaunch)

    with open(script_path, 'w') as fp:
        fp.write(script)
    st = os.stat(script_path)
    os.chmod(script_path, st.st_mode | stat.S_IEXEC)
    try:
        sched_id = scheduler.submit(script_path)
    except Exception as e:
        logger.error(f'Failed to submit job for {qlaunch}:\n{e}')
        qlaunch.delete()
        raise
    else:
        qlaunch.scheduler_id = sched_id
        qlaunch.state = "submitted"
        qlaunch.command = script_path
        qlaunch.save()
        msg = f'Submit OK: {qlaunch}'
        logger.info(msg)
        if verbose: print(msg)

def sig_handler(signum, stack):
    global EXIT_FLAG
    EXIT_FLAG = True

def get_ready_jobs():
    jobs = BalsamJob.objects.filter(
        lock='',
        queued_launch__isnull=True,
        state__in=['PREPROCESSED', 'RESTART_READY']
    )
    return jobs

def get_open_queues():
    query = QueuedLaunch.objects.values('queue').annotate(num_queued=Count('queue'))
    num_queued = {d['queue'] : d['num_queued'] for d in query}
    open_queues = {qname:queue for qname,queue in queues.queues.items()
                   if num_queued.get(qname,0) < queue['max_queued']}
    return open_queues

def main(source, args):
    signal.signal(signal.SIGINT,  sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGHUP, signal.SIG_IGN)

    if not QueuedLaunch.acquire_advisory():
        print("Another service is currently running on this Balsam DB")
        sys.exit(1)

    while not EXIT_FLAG:
        QueuedLaunch.refresh_from_scheduler()
        jobs = get_ready_jobs()
        open_queues = get_open_queues()
        if open_queues and jobs.exists():
            acquired_pks = source.acquire(jobs)
            jobs = BalsamJob.objects.filter(pk__in=acquired_pks)
            logger.info(f"Acquired {len(acquired_pks)} scheduleable BalsamJobs; open queues: {list(open_queues.keys())}")
            qlaunch = jobpacker.create_qlaunch(jobs, open_queues)
            source.release(acquired_pks)
            if qlaunch: submit_qlaunch(qlaunch)
        if not QueuedLaunch.acquire_advisory():
            logger.error('Failed to refresh advisory lock; aborting')
            break
        elif not EXIT_FLAG:
            BalsamJob.source.clear_stale_locks()
            time.sleep(10)

if __name__ == "__main__":
    config_logging('service')
    logger.info(f"Balsam Service starting on {gethostname()}")
    parser = service_subparser()
    transition_pool = transitions.TransitionProcessPool(1, '')
    source = BalsamJob.source
    source.start_tick()
    try:
        main(source, parser.parse_args())
    except: 
        raise
    finally: 
        transition_pool.terminate()
        source.release_all_owned()
        logger.info(f"Balsam Service shutdown: released all locks OK")
