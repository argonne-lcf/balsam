'''BalsamJob pre and post execution'''
from collections import namedtuple
import glob
import multiprocessing
import queue
import logging

from django.core.exceptions import ObjectDoesNotExist
from django.conf import settings
from django import db

from common import transfer
from balsam.launcher.exceptions import *
from balsam.models import BalsamJob
logger = logging.getLogger(__name__)

PREPROCESS_TIMEOUT_SECONDS = 300

StatusMsg = namedtuple('Status', ['pk', 'state', 'msg'])
JobMsg =   namedtuple('JobMsg', ['job', 'transition_function'])


def main(job_queue, status_queue):
    db.connection.close()
    while True:
        job_msg = job_queue.get()
        job, transition_function = job_msg
        if job == 'end': return

        try:
            transition_function(job)
        except BalsamTransitionError as e:
            job.update_state('FAILED', str(e))
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
        transition_function = TRANSITIONS[job.state]
        m = JobMsg(job, transition_function)
        self.job_queue.put(m)
        self.transitions_pk_list.append(job.pk)

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
    elif job.state != 'AWAITING_PARENTS':
        job.update_state('AWAITING_PARENTS', f'{len(parents)} pending jobs')


def stage_in(job):
    # Create workdirs for jobs: use job.create_working_path
    logger.debug('in stage_in')

    if not os.path.exists(job.working_directory):
        job.create_working_path()
    work_dir = job.working_directory

    # stage in all remote urls
    # TODO: stage_in remote transfer should allow a list of files and folders,
    # rather than copying just one entire folder
    url_in = job.stage_in_url
    if url_in:
        try:
            transfer.stage_in(f"{url_in}/",  f"{work_dir}/")
        except Exception as e:
            message = 'Exception received during stage_in: ' + str(e)
            logger.error(message)
            raise BalsamTransitionError from e

    # create unique symlinks to "input_files" patterns from parents
    # TODO: handle data flow from remote sites transparently
    matches = []
    parents = job.get_parents()
    input_patterns = job.input_files.split()
    for parent in parents:
        parent_dir = parent.working_directory
        for pattern in input_patterns:
            path = os.path.join(parent_dir, pattern)
            matches.extend((parent.pk, glob.glob(path)))

    for parent_pk, inp_file in matches:
        basename = os.path.basename(inp_file)
        newpath = os.path.join(work_dir, basename)
        if os.path.exists(newpath): newpath += f"_{parent_pk}"
        # pointing to src, named dst
        os.symlink(src=inp_file, dst=newpath)
    job.update_state('STAGED_IN')


def stage_out(job):
    '''copy from the local working_directory to the output_url '''
    logger.debug('in stage_out')

    stage_out_patterns = job.stage_out_files.split()
    work_dir = job.working_directory
    matches = []
    for pattern in stage_out_patterns:
        path = os.path.join(work_dir, pattern)
        matches.extend(glob.glob(path))

    if matches:
        with tempfile.TemporaryDirectory() as stagingdir:
            try:
                for f in matches: 
                    base = os.path.basename(f)
                    dst = os.path.join(stagingdir, base)
                    os.link(src=f, dst=dst)
                transfer.stage_out(f"{stagingdir}/", f"{job.stage_out_url}/")
            except Exception as e:
                message = 'Exception received during stage_out: ' + str(e)
                logger.error(message)
                raise BalsamTransitionError from e
    job.update_state('JOB_FINISHED') # this completes the transitions


def preprocess(job):
    logger.debug('in preprocess ')

    # Get preprocesser exe
    preproc_app = job.preprocess
    if not preproc_app:
        try:
            app = job.get_application()
        except ObjectDoesNotExist as e:
            message = f"application {job.application} does not exist"
            logger.error(message)
            raise BalsamTransitionError(message)
        preproc_app = app.default_preprocess
    if not preproc_app:
        job.update_state('PREPROCESSED', 'No preprocess: skipped')
        return
    if not os.path.exists(preproc_app):
        #TODO: look for preproc in the EXE directories
        message = f"Preprocessor {preproc_app} does not exist on filesystem"
        logger.error(message)
        raise BalsamTransitionError

    # Create preprocess-specific environment
    envs = job.get_envs()

    # Run preprocesser with special environment in job working directory
    out = os.path.join(job.working_directory, f"preprocess.log.pid{os.getpid()}")
    with open(out, 'wb') as fp:
        fp.write(f"# Balsam Preprocessor: {preproc_app}")
        try:
            proc = subprocess.Popen(preproc_app, stdout=fp,
                                    stderr=subprocess.STDOUT, env=envs,
                                    cwd=job.working_directory)
            retcode = proc.wait(timeout=PREPROCESS_TIMEOUT_SECONDS)
        except Exception as e:
            message = f"Preprocess failed: {e}"
            logger.error(message)
            proc.kill()
            raise BalsamTransitionError from e
    if retcode != 0:
        message = f"Preprocess Popen nonzero returncode: {retcode}"
        logger.error(message)
        raise BalsamTransitionError(message)

    job.update_state('PREPROCESSED', f"{os.path.basename(preproc_app)}")


def postprocess(job, *, error_handling=False, timeout_handling=False):
    logger.debug('in postprocess ')
    if error_handling and timeout_handling:
        raise ValueError("Both error-handling and timeout-handling is invalid")
    if error_handling: logger.debug('Handling RUN_ERROR')
    if timeout_handling: logger.debug('Handling RUN_TIMEOUT')

    # Get postprocesser exe
    postproc_app = job.postprocess
    if not postproc_app:
        try:
            app = job.get_application()
        except ObjectDoesNotExist as e:
            message = f"application {job.application} does not exist"
            logger.error(message)
            raise BalsamTransitionError(message)
        postproc_app = app.default_postprocess

    # If no postprocesssor; move on (unless in error_handling mode)
    if not postproc_app:
        if error_handling:
            message = "Trying to handle error, but no postprocessor found"
            logger.warning(message)
            raise BalsamTransitionError(message)
        elif timeout_handling:
            logger.warning('Unhandled job timeout: marking RESTART_READY')
            job.update_state('RESTART_READY', 'marking for re-run')
            return
        else:
            job.update_state('POSTPROCESSED', 'No postprocess: skipped')
            return

    if not os.path.exists(postproc_app):
        #TODO: look for postproc in the EXE directories
        message = f"Postprocessor {postproc_app} does not exist on filesystem"
        logger.error(message)
        raise BalsamTransitionError

    # Create postprocess-specific environment
    envs = job.get_envs(timeout=timeout_handling, error=error_handling)

    # Run postprocesser with special environment in job working directory
    out = os.path.join(job.working_directory, f"postprocess.log.pid{os.getpid()}")
    with open(out, 'wb') as fp:
        fp.write(f"# Balsam Postprocessor: {postproc_app}\n")
        if timeout_handling: fp.write("# Invoked to handle RUN_TIMEOUT\n")
        if error_handling: fp.write("# Invoked to handle RUN_ERROR\n")

        try:
            proc = subprocess.Popen(postproc_app, stdout=fp,
                                    stderr=subprocess.STDOUT, env=envs,
                                    cwd=job.working_directory)
            retcode = proc.wait(timeout=POSTPROCESS_TIMEOUT_SECONDS)
        except Exception as e:
            message = f"Postprocess failed: {e}"
            logger.error(message)
            proc.kill()
            raise BalsamTransitionError from e
    if retcode != 0:
        message = f"Postprocess Popen nonzero returncode: {retcode}"
        logger.error(message)
        raise BalsamTransitionError(message)

    # If postprocessor handled error or timeout, it should have changed job's
    # state. If it failed to do this, mark FAILED.  Otherwise, POSTPROCESSED.
    job.refresh_from_db()
    if error_handling and job.state == 'RUN_ERROR':
        message = f"Error handling failed to change job state: marking FAILED"
        logger.warning(message)
        raise BalsamTransitionError(message)

    if timeout_handling and job.state == 'RUN_TIMEOUT':
        message = f"Timeout handling failed to change job state: marking FAILED"
        logger.warning(message)
        raise BalsamTransitionError(message)

    if not (error_handling or timeout_handling):
        job.update_state('POSTPROCESSED', f"{os.path.basename(postproc_app)}")


def handle_timeout(job):
    if job.post_timeout_handler:
        postprocess(job, timeout_handling=True)
    elif job.auto_timeout_retry:
        job.update_state('RESTART_READY', 'timedout: auto retry')
    else:
        raise BalsamTransitionError("No timeout handling: marking FAILED")


def handle_run_error(job):
    if job.post_error_handler:
        postprocess(job, error_handling=True)
    else:
        raise BalsamTransitionError("No error handler: run failed")


TRANSITIONS = {
    'CREATED':          check_parents,
    'LAUNCHER_QUEUED':  check_parents,
    'AWAITING_PARENTS': check_parents,
    'READY':            stage_in,
    'STAGED_IN':        preprocess,
    'RUN_DONE':         postprocess,
    'RUN_TIMEOUT':      handle_timeout,
    'RUN_ERROR':        handle_run_error,
    'POSTPROCESSED':    stage_out,
}
