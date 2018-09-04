import os
import sys
import signal
import stat

import django
os.environ['DJANGO_SETTINGS_MODULE'] = 'balsam.django_config.settings'
django.setup()
from django.conf import settings

import logging
logger = logging.getLogger('balsam.service')
logger.info("Loading Balsam Service")

from balsam.service import models
from balsam.service.schedulers import script_template, scheduler
from balsam.scripts.cli import service_subparser

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

class ServiceManager:

    def __init__(self, args):
        self.delayer = delay_generator(period=5)
        self.queues = QueueManager()
        pass

    def argo_sync(self):
        '''starts thread to exchange jobs with argo server'''
        pass
    
    def update_perf_models(self):
        pass

    def assign_nodehours(self):
        pass

    def to_schedule(self):
        return BalsamJob.objects.filter(lock='',queued_launch=None)

    def refresh_qlaunches(self):
        self.queues.refresh()

        for qlaunch in models.QueuedLaunch.objects.all():
            try:
                state = self.queues.get_state(qlaunch)
            except queues.LaunchNotQueued:
                logger.info(f'{qlaunch} no longer in job queue; deleting')
                qlaunch.delete()
            else:
                if state != qlaunch.state:
                    qlaunch.state = state
                    qlaunch.save()
                    logger.info(f'{qlaunch} now in state: {state}')

    def update(self):
        next(self.delayer)
        self.argo_sync()
        self.update_perf_models()
        self.assign_nodehours()
        self.refresh_qlaunches()
        qlaunch = jobpacker.create_qlaunch(self.to_schedule(), self.queues)
        if qlaunch:
            script = JobScript(qlaunch)
            self.queues[qlaunch.qname].submit(script)

def main(args):
    signal.signal(signal.SIGINT,  sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGHUP, signal.SIG_IGN)

    QueuedLaunch = models.QueuedLaunch
    if not QueuedLaunch.acquire_advisory():
        print("Another service is currently running on this Balsam DB")
        sys.exit(1)

    manager = ServiceManager(args)

    try:
        while not EXIT_FLAG:
            manager.update()
            QueuedLaunch.acquire_advisory()
    except:
        raise
    finally:
        manager.clean_up()

if __name__ == "__main__":
    parser = service_subparser()
    main(parser.parse_args())
