'''BalsamJob pre and post execution'''
from collections import namedtuple
import logging

from django.core.exceptions import ObjectDoesNotExist

from common import transfer

logger = logging.getLogger(__name__)
class ProcessingError(Exception): pass

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


StatusMsg = namedtuple('Status', ['pk', 'state', 'msg'])
JobMsg =   namedtuple('JobMsg', ['pk', 'transition_function'])

def main(job_queue, status_queue):
    while True:
        job, process_function = job_queue.get()
        if job == 'end':
            return
        try:
            process_function(job)
        except ProcessingError as e:
            s = StatusMsg(job.pk, 'FAILED', str(e))
            status_queue.put(s)
        else:
            s = StatusMsg(job.pk, job.state, 'success')
            status_queue.put(s)
