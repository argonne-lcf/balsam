'''BalsamJob Transitions

The user selects ``BALSAM_MAX_CONCURRENT_TRANSITIONS`` processes to run
alongside the main Launcher process.  These are created with ``multiprocessing.Process``
and communicate with the Launcher through a ``multiprocessing.Queue`` (or a
PriorityQueue via a ``multiprocessing.managers.SyncManager``)

The transition processes pull jobs from the queue, execute the necessary
transition function, and signal completion by putting a status message back on a
status queue. These transition processes explicitly ignore SIGINT and SIGTERM
interrupts, so that they can finish the current transition and exit gracefully,
under control of the main Launcher process.
'''
from collections import namedtuple
import glob
import multiprocessing
import multiprocessing.managers
import queue
import os
from io import StringIO
from traceback import print_exc
import signal
import shutil
import sys
import subprocess
import tempfile

from django.core.exceptions import ObjectDoesNotExist
from django.conf import settings
from django import db

from balsam.common import transfer 
from balsam.launcher.exceptions import *
from balsam.service.models import BalsamJob, NoApplication
from balsam.launcher.util import get_tail

import logging
logger = logging.getLogger('balsam.launcher.transitions')

# SQLite exclusive lock is broken on Windows & OSX; even with two writers, two
# records, and a long timeout, a "database locked" exception is thrown
# immediately.  Writers are supposed to queue up, but this correct behavior is
# seen only on Linux.  If we are running OSX or Windows, we have to implement
# our own global DB write lock (multiprocessing.Lock object). If concurrent
# DB writes become a bottleneck, we have to go to a DB that supports better
# concurrency -- but SQLite makes it signifcantly easier for users to deploy
# Balsam, because it's built in and requires zero user configuration
if sys.platform.startswith('darwin'):
    LockClass = multiprocessing.Lock
elif sys.platform.startswith('win32'):
    LockClass = multiprocessing.Lock
else:
    class DummyLock:
        def acquire(self): pass
        def release(self): pass
    LockClass = DummyLock
LockClass = multiprocessing.Lock # TODO: replace with better solution!
logger.debug(f'Using lock: {LockClass}')

PREPROCESS_TIMEOUT_SECONDS = 300
POSTPROCESS_TIMEOUT_SECONDS = 300
SITE = settings.BALSAM_SITE

StatusMsg = namedtuple('StatusMsg', ['pk', 'state', 'msg'])

def on_exit():
    logger.debug("TransitionProc caught SIGTERM: do nothing and wait for end")

def main(job_queue, status_queue, lock):
    handler = lambda a,b: on_exit()
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    while True:
        logger.debug("Transition process waiting for job")
        job_msg = job_queue.get()
        priority, jobid = job_msg
        
        if jobid == 'end':
            logger.debug("Received end..quitting transition loop")
            return
        else:
            job = BalsamJob.objects.get(pk=jobid)
            transition_function = TRANSITIONS[job.state]

        if job.work_site != SITE:
            job.work_site = SITE
            lock.acquire()
            job.save(update_fields=['work_site'])
            lock.release()
        logger.debug(f"Received job {job.cute_id}: {transition_function} (priority {priority})")
        try:
            transition_function(job, lock)
        except BalsamTransitionError as e:
            job.refresh_from_db()
            lock.acquire()
            job.update_state('FAILED', str(e))
            lock.release()

            s = StatusMsg(job.pk, 'FAILED', str(e))
            status_queue.put(s)
            buf = StringIO()
            print_exc(file=buf)
            logger.exception(f"{job.cute_id} BalsamTransitionError:\n%s\n", buf.getvalue())
            logger.exception(f"Marking {job.cute_id} as FAILED")
        except:
            buf = StringIO()
            print_exc(file=buf)
            logger.critical(f"{job.cute_id} Uncaught exception:\n%s", buf.getvalue())
            raise
        else:
            s = StatusMsg(job.pk, str(job.state), 'success')
            status_queue.put(s)


class TransitionProcessPool:
    '''Contains the transition process objects, a job_queue PriorityQueue for adding
    new transitions, and a status_queue for monitoring completion of
    transitions'''
    NUM_PROC = settings.BALSAM_MAX_CONCURRENT_TRANSITIONS

    def __init__(self):
        '''Initialize the multiprocessing queues and start the transition
        processes'''
        # Use a priority queue to ensure pre-run transitions occur fast
        handler = lambda a,b: on_exit()
        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

        class PQueueManager(multiprocessing.managers.SyncManager): pass
        PQueueManager.register("PriorityQueue", queue.PriorityQueue)
        self.pqueue_manager = PQueueManager()
        self.pqueue_manager.start()
        self.job_queue = self.pqueue_manager.PriorityQueue()

        self.status_queue = multiprocessing.Queue()
        self.lock = LockClass()
        self.transitions_pk_list = []

        self.procs = [
            multiprocessing.Process( target=main, 
                                    args=(self.job_queue, self.status_queue, self.lock))
            for i in range(self.NUM_PROC)
        ]
        logger.debug(f"Starting {len(self.procs)} transition processes")
        db.connections.close_all()
        for proc in self.procs: 
            proc.daemon = True
            proc.start()

    def __contains__(self, job):
        '''Check if a job is currently in a transition'''
        return job.pk in self.transitions_pk_list

    def add_job(self, job):
        '''Add job to the job_queue.  A transition process will pull this job
        off the queue and carry out the transition.  

        The job primary key is added to ``transitions_pk_list`` so that the Launcher can
        easily check which jobs are currently in transition
        '''
        if job in self: raise BalsamTransitionError("already in transition")
        if job.state not in TRANSITIONS: raise TransitionNotFoundError
        
        priority = PRIORITIES[job.state]
        job_msg = (priority, job.pk)
        self.job_queue.put(job_msg)
        self.transitions_pk_list.append(job.pk)

    def get_statuses(self):
        '''Generator: pull statuses off the status queue until it's exhausted.

        While the Launcher does not need to do anything with these status
        messages, iterating over this object will clear ``transitions_pk_list``
        which is used by the Launcher to identify jobs in transition.'''
        while not self.status_queue.empty():
            try:
                stat = self.status_queue.get_nowait()
                self.transitions_pk_list.remove(stat.pk)
                yield stat
            except queue.Empty:
                break

    def end_and_wait(self):
        '''Flush the queue (or for priority queue, place a high-priority END
        message) in the queue. Allow all transition processes to finish the
        current step before shutdown.'''
        priority = PRIORITIES['end']
        job_msg = (priority, 'end')
        logger.debug("Sending end message and waiting on transition processes")
        for proc in self.procs:
            self.job_queue.put(job_msg)
        for proc in self.procs: 
            proc.join()
        self.pqueue_manager.shutdown()
        logger.info("All Transition processes joined: done.")
        self.transitions_pk_list = []

def stage_in(job, lock):
    # Create workdirs for jobs: use job.create_working_path
    logger.debug(f'{job.cute_id} in stage_in')

    if not os.path.exists(job.working_directory):
        lock.acquire()
        job.create_working_path()
        lock.release()
    work_dir = job.working_directory
    logger.info(f"{job.cute_id} working directory {work_dir}")

    # stage in all remote urls
    # TODO: stage_in remote transfer should allow a list of files and folders,
    # rather than copying just one entire folder
    url_in = job.stage_in_url
    if url_in:
        logger.info(f"{job.cute_id} transfer in from {url_in}")
        try:
            transfer.stage_in(f"{url_in}/",  f"{work_dir}/")
        except Exception as e:
            message = 'Exception received during stage_in: ' + str(e)
            raise BalsamTransitionError(message) from e

    # create unique symlinks to "input_files" patterns from parents
    # TODO: handle data flow from remote sites transparently
    matches = []
    parents = job.get_parents()
    input_patterns = job.input_files.split()
    logger.debug(f"{job.cute_id} searching parent workdirs for {input_patterns}")
    for parent in parents:
        parent_dir = parent.working_directory
        for pattern in input_patterns:
            path = os.path.join(parent_dir, pattern)
            matches.extend((parent.pk,match) 
                           for match in glob.glob(path))

    for parent_pk, inp_file in matches:
        basename = os.path.basename(inp_file)
        new_path = os.path.join(work_dir, basename)
        
        if os.path.exists(new_path): new_path += f"_{str(parent_pk)[:8]}"
        # pointing to src, named dst
        logger.info(f"{job.cute_id}   {new_path}  -->  {inp_file}")
        try:
            os.symlink(src=inp_file, dst=new_path)
        except Exception as e:
            raise BalsamTransitionError(
                f"Exception received during symlink: {e}") from e

    lock.acquire()
    job.update_state('STAGED_IN')
    lock.release()
    logger.info(f"{job.cute_id} stage_in done")


def stage_out(job, lock):
    '''copy from the local working_directory to the output_url '''
    logger.debug(f'{job.cute_id} in stage_out')

    url_out = job.stage_out_url
    if not url_out:
        lock.acquire()
        job.update_state('JOB_FINISHED')
        lock.release()
        logger.info(f'{job.cute_id} no stage_out_url: done')
        return

    stage_out_patterns = job.stage_out_files.split()
    logger.debug(f"{job.cute_id} stage out files match: {stage_out_patterns}")
    work_dir = job.working_directory
    matches = []
    for pattern in stage_out_patterns:
        path = os.path.join(work_dir, pattern)
        matches.extend(glob.glob(path))

    if matches:
        logger.info(f"{job.cute_id} stage out files: {matches}")
        with tempfile.TemporaryDirectory() as stagingdir:
            try:
                for f in matches: 
                    base = os.path.basename(f)
                    dst = os.path.join(stagingdir, base)
                    shutil.copyfile(src=f, dst=dst)
                    logger.info(f"staging {f} out for transfer")
                logger.info(f"transferring to {url_out}")
                transfer.stage_out(f"{stagingdir}/", f"{url_out}/")
            except Exception as e:
                message = f'Exception received during stage_out: {e}'
                raise BalsamTransitionError(message) from e
    lock.acquire()
    job.update_state('JOB_FINISHED')
    lock.release()
    logger.info(f'{job.cute_id} stage_out done')


def preprocess(job, lock):
    logger.debug(f'{job.cute_id} in preprocess')

    # Get preprocesser exe
    preproc_app = job.preprocess
    if not preproc_app:
        try:
            app = job.get_application()
            preproc_app = app.default_preprocess
        except ObjectDoesNotExist as e:
            message = f"application {job.application} does not exist"
            raise BalsamTransitionError(message)
        except NoApplication:
            preproc_app = None
    if not preproc_app:
        lock.acquire()
        job.update_state('PREPROCESSED', 'No preprocess: skipped')
        lock.release()
        logger.info(f"{job.cute_id} no preprocess: skipped")
        return
    if not os.path.exists(preproc_app.split()[0]):
        #TODO: look for preproc in the EXE directories
        message = f"Preprocessor {preproc_app} does not exist on filesystem"
        raise BalsamTransitionError(message)

    # Create preprocess-specific environment
    envs = job.get_envs()

    # Run preprocesser with special environment in job working directory
    out = os.path.join(job.working_directory, f"preprocess.log")
    with open(out, 'w') as fp:
        fp.write(f"# Balsam Preprocessor: {preproc_app}")
        fp.flush()
        try:
            args = preproc_app.split()
            logger.info(f"{job.cute_id} preprocess Popen {args}")
            lock.acquire()
            proc = subprocess.Popen(args, stdout=fp,
                                    stderr=subprocess.STDOUT, env=envs,
                                    cwd=job.working_directory)
            retcode = proc.wait(timeout=PREPROCESS_TIMEOUT_SECONDS)
            proc.communicate()
            lock.release()
        except Exception as e:
            message = f"Preprocess failed: {e}"
            proc.kill()
            raise BalsamTransitionError(message) from e

    job.refresh_from_db()
    if retcode != 0:
        tail = get_tail(out)
        message = f"{job.cute_id} preprocess returned {retcode}:\n{tail}"
        raise BalsamTransitionError(message)

    lock.acquire()
    job.update_state('PREPROCESSED', f"{os.path.basename(preproc_app)}")
    lock.release()
    logger.info(f"{job.cute_id} preprocess done")

def postprocess(job, lock, *, error_handling=False, timeout_handling=False):
    logger.debug(f'{job.cute_id} in postprocess')
    if error_handling and timeout_handling:
        raise ValueError("Both error-handling and timeout-handling is invalid")
    if error_handling: logger.info(f'{job.cute_id} handling RUN_ERROR')
    if timeout_handling: logger.info(f'{job.cute_id} handling RUN_TIMEOUT')

    # Get postprocesser exe
    postproc_app = job.postprocess
    if not postproc_app:
        try:
            app = job.get_application()
            postproc_app = app.default_postprocess
        except ObjectDoesNotExist as e:
            message = f"application {job.application} does not exist"
            logger.error(message)
            raise BalsamTransitionError(message)
        except NoApplication:
            postproc_app = None

    # If no postprocesssor; move on (unless in error_handling mode)
    if not postproc_app:
        if error_handling:
            message = f"{job.cute_id} handle error: no postprocessor found!"
            raise BalsamTransitionError(message)
        elif timeout_handling:
            lock.acquire()
            job.update_state('RESTART_READY', 'marking for re-run')
            lock.release()
            logger.warning(f'{job.cute_id} unhandled job timeout: marked RESTART_READY')
            return
        else:
            lock.acquire()
            job.update_state('POSTPROCESSED',
                             f'{job.cute_id} no postprocess: skipped')
            lock.release()
            logger.info(f'{job.cute_id} no postprocess: skipped')
            return

    if not os.path.exists(postproc_app.split()[0]):
        #TODO: look for postproc in the EXE directories
        message = f"Postprocessor {postproc_app} does not exist on filesystem"
        raise BalsamTransitionError(message)

    # Create postprocess-specific environment
    envs = job.get_envs(timeout=timeout_handling, error=error_handling)

    # Run postprocesser with special environment in job working directory
    out = os.path.join(job.working_directory, f"postprocess.log")
    with open(out, 'w') as fp:
        fp.write(f"# Balsam Postprocessor: {postproc_app}\n")
        if timeout_handling: fp.write("# Invoked to handle RUN_TIMEOUT\n")
        if error_handling: fp.write("# Invoked to handle RUN_ERROR\n")
        fp.flush()
        
        try:
            args = postproc_app.split()
            logger.info(f"{job.cute_id} postprocess Popen {args}")
            lock.acquire()
            proc = subprocess.Popen(args, stdout=fp,
                                    stderr=subprocess.STDOUT, env=envs,
                                    cwd=job.working_directory)
            retcode = proc.wait(timeout=POSTPROCESS_TIMEOUT_SECONDS)
            proc.communicate()
            lock.release()
        except Exception as e:
            message = f"Postprocess failed: {e}"
            proc.kill()
            raise BalsamTransitionError(message) from e
    
    if retcode != 0:
        tail = get_tail(out)
        message = f"{job.cute_id} postprocess returned {retcode}:\n{tail}"
        raise BalsamTransitionError(message)

    job.refresh_from_db()
    # If postprocessor handled error or timeout, it should have changed job's
    # state. If it failed to do this, mark FAILED.  Otherwise, POSTPROCESSED.
    if error_handling and job.state == 'RUN_ERROR':
        message = f"{job.cute_id} Error handling didn't fix job state: marking FAILED"
        raise BalsamTransitionError(message)

    if timeout_handling and job.state == 'RUN_TIMEOUT':
        message = f"{job.cute_id} Timeout handling didn't change job state: marking FAILED"
        raise BalsamTransitionError(message)

    if not (error_handling or timeout_handling):
        lock.acquire()
        job.update_state('POSTPROCESSED', f"{os.path.basename(postproc_app)}")
        lock.release()
    logger.info(f"{job.cute_id} postprocess done")


def handle_timeout(job, lock):
    logger.debug(f'{job.cute_id} in handle_timeout')
    if job.post_timeout_handler:
        logger.debug(f'{job.cute_id} invoking postprocess with timeout_handling flag')
        postprocess(job, lock, timeout_handling=True)
    elif job.auto_timeout_retry:
        logger.info(f'{job.cute_id} marking RESTART_READY')
        lock.acquire()
        job.update_state('RESTART_READY', 'timedout: auto retry')
        lock.release()
    else:
        raise BalsamTransitionError(f"{job.cute_id} no timeout handling: marking FAILED")


def handle_run_error(job, lock):
    logger.debug(f'{job.cute_id} in handle_run_error')
    if job.post_error_handler:
        logger.debug(f'{job.cute_id} invoking postprocess with error_handling flag')
        postprocess(job, lock, error_handling=True)
    else:
        raise BalsamTransitionError("No error handler: run failed")


TRANSITIONS = {
    'READY':            stage_in,
    'STAGED_IN':        preprocess,
    'RUN_DONE':         postprocess,
    'RUN_TIMEOUT':      handle_timeout,
    'RUN_ERROR':        handle_run_error,
    'POSTPROCESSED':    stage_out,
}
PRIORITIES = {
    'end' : -1, # highest priority
    'READY':            0,
    'STAGED_IN':        0,
    'RUN_TIMEOUT':      1,
    'RUN_ERROR':        1,
    'RUN_DONE':         2,
    'POSTPROCESSED':    2,
}
