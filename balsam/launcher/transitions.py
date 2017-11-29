'''BalsamJob pre and post execution'''
from collections import namedtuple
import logging

from django.core.exceptions import ObjectDoesNotExist
from django.conf import settings
from django import db

from common import transfer
from balsam.launcher.exceptions import *
logger = logging.getLogger(__name__)


StatusMsg = namedtuple('Status', ['pk', 'state', 'msg'])
JobMsg =   namedtuple('JobMsg', ['pk', 'transition_function'])


def main(job_queue, status_queue):
    db.connection.close()
    while True:
        job, process_function = job_queue.get()
        if job == 'end': return

        try:
            process_function(job)
        except BalsamTransitionError as e:
            s = StatusMsg(job.pk, 'FAILED', str(e))
            status_queue.put(s)
        else:
            s = StatusMsg(job.pk, job.state, 'success')
            status_queue.put(s)


class TransitionProcessPool:
    
    NUM_PROC = settings.BALSAM_MAX_CONCURRENT_TRANSITIONS

    def __init__(self):
        
        self.job_queue = multiprocessing.Queue()
        self.status_queue = multiprocessing.Queue()
        self.transitions_pk_list = []

        self.procs = [
            multiprocessing.Process( target=main, 
                                    args=(self.job_queue, self.status_queue))
            for i in range(NUM_PROC)
        ]
        db.connections.close_all()
        for proc in self.procs: proc.start()

    def __contains__(self, job):
        return job.pk in self.transitions_pk_list

    def add_job(self, job):
        if job in self: raise BalsamTransitionError("already in transition")
        if job.state not in TRANSITIONS: raise TransitionNotFoundError
        pk = job.pk
        transition_function = TRANSITIONS[job.state]
        m = JobMsg(pk, transition_function)
        self.job_queue.put(m)
        self.transitions_pk_list.append(pk)

    def get_statuses():
        while not self.status_queue.empty():
            try:
                stat = self.status_queue.get_nowait()
                self.transitions_pk_list.remove(stat.pk)
                yield stat
            except queue.Empty:
                break

    def flush_job_queue(self):
        while not self.job_queue.empty():
            try:
                self.job_queue.get_nowait()
            except queue.Empty:
                break

    def end_and_wait(self):
        m = JobMsg('end', None)
        for proc in self.procs:
            self.job_queue.put(m)
        for proc in self.procs: proc.wait()
        self.transitions_pk_list = []


def check_parents(job):
    parents = job.get_parents()
    ready = all(p.state == 'JOB_FINISHED' for p in parents)
    if ready:
        job.update_state('READY', 'dependencies satisfied')
    elif job.state == 'CREATED':
        job.update_state('NOT_READY', 'awaiting dependencies')


def stage_in(job):
    # Create workdirs for jobs: use job.create_working_path
    logger.debug('in stage_in')
    job.update_state('STAGING_IN')

    if not os.path.exists(job.working_directory):
        job.create_working_path()

    if job.input_url != '':
        try:
            transfer.stage_in(job.input_url + '/', job.working_directory + '/')
            job.state = STAGED_IN.name
        except Exception as e:
            message = 'Exception received during stage_in: ' + str(e)
            logger.error(message)
            job.state = STAGE_IN_FAILED.name
    else:
        # no input url specified so stage in is complete
        job.state = STAGED_IN.name

    job.update_state('STAGE_IN_DONE')

def stage_out(job):
    ''' if the job has files defined via the output_files and an output_url is defined,
        they are copied from the local working_directory to the output_url '''
    logger.debug('in stage_out')
    message = None
    if job.output_url != '':
        try:
            transfer.stage_out(
                job.working_directory + '/',
                job.output_url + '/')
            job.state = STAGED_OUT.name
        except Exception as e:
            message = 'Exception received during stage_out: ' + str(e)
            logger.error(message)
            job.state = STAGE_OUT_FAILED.name
    else:
        # no output url specififed so stage out is complete
        job.state = STAGED_OUT.name

    job.save(
        update_fields=['state'],
        using=db_tools.get_db_connection_id(
            job.pk))
    status_sender = BalsamStatusSender.BalsamStatusSender(
        settings.SENDER_CONFIG)
    status_sender.send_status(job, message)

# preprocess a job


def preprocess(job):
    ''' Each job defines a task to perform, so tasks need preprocessing to prepare
        for the job to be submitted to the batch queue. '''
    logger.debug('in preprocess ')
    message = 'Job prepocess complete.'
    # get the task that is running
    try:
        app = ApplicationDefinition.objects.get(name=job.application)
        if app.preprocess:
            if os.path.exists(app.preprocess):
                stdout = run_subprocess.run_subprocess(app.preprocess)
                # write stdout to log file
                f = open(os.path.join(job.working_directory, app.name +
                                      '.preprocess.log.pid' + str(os.getpid())), 'wb')
                f.write(stdout)
                f.close()
                job.state = PREPROCESSED.name
            else:
                message = ('Preprocess, "' + app.preprocess + '", of application, "' + str(job.application)
                           + '", does not exist on filesystem.')
                logger.error(message)
                job.state = PREPROCESS_FAILED.name
        else:
            logger.debug('No preprocess specified for this job; skipping')
            job.state = PREPROCESSED.name
    except run_subprocess.SubprocessNonzeroReturnCode as e:
        message = ('Preprocess, "' + app.preprocess + '", of application, "' + str(job.application)
                   + '", exited with non-zero return code: ' + str(returncode))
        logger.error(message)
        job.state = PREPROCESS_FAILED.name
    except run_subprocess.SubprocessFailed as e:
        message = ('Received exception while running preprocess, "' + app.preprocess
                   + '", of application, "' + str(job.application) + '", exception: ' + str(e))
        logger.error(message)
        job.state = PREPROCESS_FAILED.name
    except ObjectDoesNotExist as e:
        message = 'application,' + str(job.application) + ', does not exist.'
        logger.error(message)
        job.state = PREPROCESS_FAILED.name
    except Exception as e:
        message = 'Received exception while in preprocess, "' + \
            app.preprocess + '", for application ' + str(job.application)
        logger.exception(message)
        job.state = PREPROCESS_FAILED.name

    job.save(
        update_fields=['state'],
        using=db_tools.get_db_connection_id(
            job.pk))
    status_sender = BalsamStatusSender.BalsamStatusSender(
        settings.SENDER_CONFIG)
    status_sender.send_status(job, message)

# perform any post job processing needed
def postprocess(job):
    ''' some jobs need to have some postprocessing performed,
        this function does this.'''
    logger.debug('in postprocess ')
    message = 'Job postprocess complete'
    try:
        app = ApplicationDefinition.objects.get(name=job.application)
        if app.postprocess:
            if os.path.exists(app.postprocess):
                stdout = run_subprocess.run_subprocess(app.postprocess)
                # write stdout to log file
                f = open(os.path.join(job.working_directory, app.name +
                                      '.postprocess.log.pid' + str(os.getpid())), 'wb')
                f.write(stdout)
                f.close()
                job.state = POSTPROCESSED.name
            else:
                message = ('Postprocess, "' + app.postprocess + '", of application, "' + str(job.application)
                           + '", does not exist on filesystem.')
                logger.error(message)
                job.state = POSTPROCESS_FAILED.name
        else:
            logger.debug('No postprocess specified for this job; skipping')
            job.state = POSTPROCESSED.name
    except run_subprocess.SubprocessNonzeroReturnCode as e:
        message = ('Postprocess, "' + app.postprocess + '", of application, "' + str(job.application)
                   + '", exited with non-zero return code: ' + str(returncode))
        logger.error(message)
        job.state = POSTPROCESS_FAILED.name
    except run_subprocess.SubprocessFailed as e:
        message = ('Received exception while running postprocess, "' + app.preprocess
                   + '", of application, "' + str(job.application) + '", exception: ' + str(e))
        logger.error(message)
        job.state = POSTPROCESS_FAILED.name
    except ObjectDoesNotExist as e:
        message = 'application,' + str(job.application) + ', does not exist.'
        logger.error(message)
        job.state = POSTPROCESS_FAILED.name
    except Exception as e:
        message = 'Received exception while in postprocess, "' + \
            app.postprocess + '", for application ' + str(job.application)
        logger.error(message)
        job.state = POSTPROCESS_FAILED.name

    job.save(
        update_fields=['state'],
        using=db_tools.get_db_connection_id(
            job.pk))
    status_sender = BalsamStatusSender.BalsamStatusSender(
        settings.SENDER_CONFIG)
    status_sender.send_status(job, message)

TRANSITIONS = {
    'CREATED':          check_parents,
    'LAUNCHER_QUEUED':  check_parents,
    'AWAITING_PARENTS': check_parents,
    'READY':            stage_in,
    'STAGED_IN':        preprocess,
    'RUN_DONE':         postprocess,
    'RUN_TIMEOUT':      postprocess,
    'RUN_ERROR':        postprocess,
    'POSTPROCESSED':    stage_out,
}
