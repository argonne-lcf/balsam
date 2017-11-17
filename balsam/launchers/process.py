'''BalsamJob pre and post execution Transitions'''
import logging
from common import transfer, MessageInterface, run_subprocess
from common import db_tools
logger = logging.getLogger(__name__)

from balsam import BalsamStatusSender
#from django.db import utils, connections, DEFAULT_DB_ALIAS
from django.core.exceptions import ObjectDoesNotExist
from balsam.schedulers import exceptions

def main(job_queue, status_queue):
    while True:
        job, process_function = job_queue.get()
        process_function(job)
        

def check_parents(job):
    pass


def stage_in(job):
    ''' if the job an input_url defined,
        the files are copied to the local working_directory '''
    logger.debug('in stage_in')
    message = 'job staged in'
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

    job.save(
        update_fields=['state'],
        using=db_tools.get_db_connection_id(
            job.pk))
    status_sender = BalsamStatusSender.BalsamStatusSender(
        settings.SENDER_CONFIG)
    status_sender.send_status(job, message)

# stage out files for a job


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

# submit the job to the local scheduler


def submit(job):
    ''' this function submits the job to the local batch system '''
    logger.debug('in submit')
    message = ''
    try:
        # some schedulers have limits on the number of jobs that can
        # be queued, so check to see if we are at that number
        # If so, don't submit the job
        jobs_queued = BalsamJob.objects.filter(state=QUEUED.name)
        if len(jobs_queued) <= settings.BALSAM_MAX_QUEUED:
            cmd = job.get_application_command()
            scheduler.submit(job, cmd)
            job.state = SUBMITTED.name
            message = 'Job entered SUBMITTED state'
        else:
            message = 'Job submission delayed due to local queue limits'
    except exceptions.SubmitNonZeroReturnCode as e:
        message = 'scheduler returned non-zero value during submit command: ' + \
            str(e)
        logger.error(message)
        job.state = SUBMIT_FAILED.name
    except exceptions.SubmitSubprocessFailed as e:
        message = 'subprocess in scheduler submit failed: ' + str(e)
        logger.error(message)
        job.state = SUBMIT_FAILED.name
    except exceptions.JobSubmissionDisabled as e:
        message = 'scheduler job submission is currently disabled: ' + str(e)
        logger.error(message)
        job.state = SUBMIT_DISABLED.name
    except Exception as e:
        message = 'received exception while calling scheduler submit for job ' + \
            str(job.job_id) + ', exception: ' + str(e)
        logger.exception(message)
        job.state = SUBMIT_FAILED.name

    job.save(update_fields=['state', 'scheduler_id'],
             using=db_tools.get_db_connection_id(job.pk))
    logger.debug('sending status message')
    status_sender = BalsamStatusSender.BalsamStatusSender(
        settings.SENDER_CONFIG)
    status_sender.send_status(job, message)
    logger.debug('submit done')


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


def finish_job(job):
    ''' simply change state to Finished and send status to user '''
    job.state = JOB_FINISHED.name
    job.save(
        update_fields=['state'],
        using=db_tools.get_db_connection_id(
            job.pk))
    message = "Success!"
    status_sender = BalsamStatusSender.BalsamStatusSender(
        settings.SENDER_CONFIG)
    status_sender.send_status(job, message)
