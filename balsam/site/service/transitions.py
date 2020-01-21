from collections import defaultdict
import glob
import multiprocessing
import threading
import os
from io import StringIO
from traceback import print_exc
import random
import signal
import shutil
import subprocess
import queue
import time
import tempfile

from django import db
from django.utils import timezone
from django.db.models.functions import Cast, Substr
from django.db.models import CharField

from balsam.core import transfer 
from balsam.launcher.exceptions import *
try:
    from balsam.core.models import BalsamJob
except:
    from balsam.launcher import dag
    BalsamJob = dag.BalsamJob

from balsam.core.models import PROCESSABLE_STATES, END_STATES
from balsam.launcher.util import get_tail

import logging
logger = logging.getLogger(__name__)
    
EXIT_FLAG = False

def handler(signum, stack):
    global EXIT_FLAG
    EXIT_FLAG = True

class ProcessingThreads:

    def __init__(self, num_threads):
        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)
        db.connections.close_all()
        
        self.status_queue = queue.Queue()
        self.processing_queue = queue.Queue()
        self.processing_pks = []

        self.source_thread = threading.Thread(target=self.thread_producer, args=(processing_queue,))
        self.updater_thread = threading.Thread(target=self.thread_status_updater, args=(status_queue,))
        self.worker_threads = [
            threading.Thread(target=self.thread_task_processor, args=(processing_queue,status_queue))
            for i in range(num_threads)
        ]
        self.source_thread.start()
        self.updater_thread.start()
        for worker in self.worker_threads: worker.start()
        logger.info(f'Started {num_threads} worker threads')


    def stop(self):
        global EXIT_FLAG
        EXIT_FLAG = True
        self.source_thread.join()
        logger.info('Source thread joined')
        self.status_queue.join()
        logger.info('Status queue joined')
        self.updater_thread.join()
        logger.info('Updater thread joined')
        for i, worker in enumerate(self.worker_threads):
            worker.join()
            logger.info(f'Processer thread {i} joined')

    @staticmethod
    def perform_update(msg):
        pk = msg.pop('pk')
        job = BalsamJob.objects.get(pk=pk)

        update_method = msg.pop('state')
        method = getattr(job, update_method)
        method(**msg)

    def thread_status_updater(self, status_queue):
        while not EXIT_FLAG:
            while True:
                try:
                    status_msg = status_queue.get(timeout=1)
                    self.perform_update(status_msg)
                    status_queue.task_done()
                    self.processing_pks.remove(status_msg['pk'])
                except queue.Empty:
                    break

    def thread_task_processor(self):
        while not EXIT_FLAG:
            try:
                msg = self.processing_queue.get(timeout=1)
                result = self.process_job(msg)
                self.status_queue.put(result)
                self.processing_queue.task_done()
            except queue.Empty:
                pass

    def thread_producer(self, processing_queue):
        while not EXIT_FLAG:
            to_acquire = max(0, 1000 - processing_queue.qsize())
            if to_acquire:
                processable = BalsamJob.objects.get_processable()
                processable = processable.exclude(pk__in=self.processing_pks)[:to_acquire]
                pk_list = processable.values_list('pk', flat=True)
                self.processing_pks.extend(pk_list)
                for pk in pk_list:
                    processing_queue.put(pk)
            time.sleep(1)

    @staticmethod
    def status_msg(job, state, msg=''):
        return {
            "pk": job.pk,
            "state": state,
            "message": msg,
            "timestamp": timezone.now(),
        }

    @staticmethod
    def process_job(job_msg):
        job = BalsamJob.objects.get(pk=job_msg['pk'])
        state = job.state

        if state == 'STAGED_IN':
            return run_preprocess(job)
        elif state == 'RUN_DONE':
            return run_postprocess(job)
        elif state == 'RUN_ERROR':
            return run_postprocess(job, error_handling=True)
        elif state == 'RUN_TIMEOUT':
            return run_postprocess(job, timeout_handling=True)


def run_preprocess(job):
    if not job.preprocess:
        return ProcessingThreads.status_msg(job, state='PREPROCESSED', msg='Skipped preprocess')

    envs = job.get_envs()
    out = os.path.join(job.working_directory, f'preprocess.log')
    with open(out, 'w') as fp:
        completed_proc = subprocess.run(
            job.preprocess.split(),
            stdout=fp, 
            stderr=subprocess.STDOUT, 
            env=envs,
            cwd=job.working_directory, 
            encoding='utf-8'
        )
    retcode = completed_proc.returncode
    if retcode != 0:
        tail = get_tail(out)
        return status_msg(job, state='FAILED', msg=f'Preprocess returned {retcode}:\n{tail}')
    else:
        return status_msg(job, state='PREPROCESSED', msg='Preprocess returned 0')

def run_postprocess(job, *, error_handling=False, timeout_handling=False):
    logger.debug(f'{job.cute_id} in postprocess')
    if error_handling and timeout_handling:
        raise ValueError("Both error-handling and timeout-handling is invalid")
    if error_handling: logger.info(f'{job.cute_id} handling RUN_ERROR')
    if timeout_handling: logger.info(f'{job.cute_id} handling RUN_TIMEOUT')

    # Get postprocesser exe
    postproc_app = job.postprocess

    # If no postprocesssor; move on (unless in error_handling mode)
    if not postproc_app:
        if error_handling:
            message = f"{job.cute_id} handle error: no postprocessor found!"
            raise BalsamTransitionError(message)
        elif timeout_handling:
            job.state = 'RESTART_READY'
            logger.warning(f'{job.cute_id} unhandled job timeout: marked RESTART_READY')
            return
        else:
            job.state = 'POSTPROCESSED',
            logger.debug(f'{job.cute_id} no postprocess: skipped')
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
            proc = subprocess.Popen(args, stdout=fp,
                                    stderr=subprocess.STDOUT, env=envs,
                                    cwd=job.working_directory,
                                    )
            retcode = proc.wait(timeout=POSTPROCESS_TIMEOUT_SECONDS)
            proc.communicate()
        except Exception as e:
            message = f"Postprocess failed: {e}"
            try: proc.kill()
            except: pass
            raise BalsamTransitionError(message) from e
    
    if retcode != 0:
        tail = get_tail(out, nlines=30)
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

    # Only move the state along to POSTPROCESSED if the job is still in RUN_DONE
    # and the post.py returned normally.  Otherwise, post.py might mark a job
    # FAILED, and you override it with POSTPROCESSED, breaking the workflow.
    if job.state == 'RUN_DONE':
        job.state = 'POSTPROCESSED'
    logger.debug(f"{job.cute_id} postprocess done")