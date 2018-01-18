'''Main Launcher entry point

The ``main()`` function contains the Launcher service loop, in which:
    1. Transitions are checked for completion and jobs states are updated
    2. Dependencies of waiting jobs are checked
    3. New transitions are added to the transitions queue
    4. The RunnerGroup is checked for jobs that have stopped execution
    5. A new Runner is created according to logic in create_next_runner

The ``on_exit()`` handler is invoked either when the time limit is exceeded or
if the program receives a SIGTERM or SIGINT. This takes the necessary cleanup
actions and is guaranteed to execute only once through the HANDLING_EXIT global
flag.
'''
import argparse
from math import floor
import os
import sys
import signal
import time

import django
os.environ['DJANGO_SETTINGS_MODULE'] = 'balsam.django_config.settings'
django.setup()
from django.conf import settings

import logging
logger = logging.getLogger('balsam.launcher')
logger.info("Loading Balsam Launcher")

from balsam.service.schedulers import Scheduler
from balsam.service.models import END_STATES

scheduler = Scheduler.scheduler_main

from balsam.launcher import jobreader
from balsam.launcher import transitions
from balsam.launcher import worker
from balsam.launcher import runners
from balsam.launcher.exceptions import *
from balsam.service import db_writer

ALMOST_RUNNABLE_STATES = ['READY','STAGED_IN']
RUNNABLE_STATES = ['PREPROCESSED', 'RESTART_READY']
WAITING_STATES = ['CREATED', 'LAUNCHER_QUEUED', 'AWAITING_PARENTS']
HANDLING_EXIT = False

def delay_generator(period=settings.BALSAM_SERVICE_PERIOD):
    '''Generator: Block for ``period`` seconds since the last call to __next__()'''
    nexttime = time.time() + period
    while True:
        now = time.time()
        tosleep = nexttime - now
        if tosleep <= 0:
            nexttime = now + period
        else:
            time.sleep(tosleep)
            nexttime = now + tosleep + period
        yield

def elapsed_time_minutes():
    '''Generator: yield elapsed time in minutes since first call to __next__'''
    start = time.time()
    while True:
        yield (time.time() - start) / 60.0

def remaining_time_minutes(time_limit_minutes=0.0):
    '''Generator: yield remaining time for Launcher execution

    If time_limit_minutes is given, use internal timer to count down remaining
    time. Otherwise, query scheduler for remaining time.

    Args:
        - ``time_limit_minutes`` (*float*): runtime limit
    Yields:
        - ``remaining`` (*float*): remaining time
    Raises:
        - ``StopIteration``: when there is no time left
    '''
    elapsed_timer = elapsed_time_minutes()
    while True:
        if time_limit_minutes > 0.0:
            remaining = time_limit_minutes - next(elapsed_timer)
        else:
            remaining = scheduler.remaining_time_seconds() / 60.0
        if remaining > 0: yield remaining
        else: break

def check_parents(job, lock):
    '''Check job's dependencies, update to READY if satisfied'''
    job.refresh_from_db()
    parents = job.get_parents()
    ready = all(p.state == 'JOB_FINISHED' for p in parents)
    if ready:
        lock.acquire()
        job.update_state('READY', 'dependencies satisfied')
        lock.release()
        logger.info(f'{job.cute_id} ready')
    elif job.state != 'AWAITING_PARENTS':
        lock.acquire()
        job.update_state('AWAITING_PARENTS', f'{len(parents)} parents')
        lock.release()
        logger.info(f'{job.cute_id} waiting for parents')

def log_time(minutes_left):
    '''Pretty log of remaining time'''
    if minutes_left > 1e12:
        return
    whole_minutes = floor(minutes_left)
    whole_seconds = round((minutes_left - whole_minutes)*60)
    time_str = f"{whole_minutes:02d} min : {whole_seconds:02d} sec remaining"
    logger.info(time_str)

def create_runner(jobs, runner_group, worker_group, remaining_minutes, last_runner_created):
    '''Decide whether or not to create another runner. Considers how many jobs
    can run, how many can *almost* run, how long since the last Runner was
    created, and how many jobs are serial as opposed to MPI.
    '''
    runnable_jobs = [
        job for job in jobs
        if job.pk not in runner_group.running_job_pks and
        job.state in RUNNABLE_STATES and
        job.wall_time_minutes <= remaining_minutes
    ]
    logger.debug(f"Have {len(runnable_jobs)} runnable jobs")

    # If nothing is getting pre-processed, don't bother waiting
    almost_runnable = any(j.state in ALMOST_RUNNABLE_STATES for j in jobs)

    # If it has been runner_create_period seconds, don't wait any more
    runner_create_period = settings.BALSAM_RUNNER_CREATION_PERIOD_SEC
    now = time.time()
    runner_ready = bool(now - last_runner_created > runner_create_period)

    # If there are enough serial jobs, don't wait to run
    num_serial = len([j for j in runnable_jobs if j.num_ranks == 1])
    worker = worker_group[0]
    max_serial_per_ensemble = 2 * worker.num_nodes * worker.max_ranks_per_node
    ensemble_ready = (num_serial >= max_serial_per_ensemble) or (num_serial == 0)

    if runnable_jobs:
        if runner_ready or not almost_runnable or ensemble_ready:
            try:
                runner_group.create_next_runner(runnable_jobs, worker_group)
            except ExceededMaxRunners:
                logger.info("Exceeded max concurrent runners; waiting")
            except NoAvailableWorkers:
                logger.info("Not enough idle workers to start any new runs")
            else:
                last_runner_created = now
    return last_runner_created

def main(args, transition_pool, runner_group, job_source):
    '''Main Launcher service loop'''
    delay_sleeper = delay_generator()
    last_runner_created = time.time()
    remaining_timer = remaining_time_minutes(args.time_limit_minutes)

    for remaining_minutes in remaining_timer:

        logger.info("\n******************\n"
                       "BEGIN SERVICE LOOP\n"
                       "******************")
        log_time(remaining_minutes)
        delay = True

        # Update after any finished transitions
        for stat in transition_pool.get_statuses(): delay = False
        job_source.refresh_from_db()

        # Update jobs awaiting dependencies
        waiting_jobs = (j for j in job_source.jobs if j.state in WAITING_STATES)
        for job in waiting_jobs: 
            check_parents(job, transition_pool.lock)
        
        # Enqueue new transitions
        transitionable_jobs = [
            job for job in job_source.jobs
            if job not in transition_pool
            and job.state in transitions.TRANSITIONS
        ]

        for job in transitionable_jobs:
            transition_pool.add_job(job)
            delay = False
            fxn = transitions.TRANSITIONS[job.state]
            logger.info(f"Queued transition: {job.cute_id} will undergo {fxn}")
        
        # Update jobs that are running/finished
        any_finished = runner_group.update_and_remove_finished()
        if any_finished: delay = False
        job_source.refresh_from_db()
    
        # Decide whether or not to start a new runner
        last_runner_created = create_runner(job_source.jobs, runner_group, 
                                             worker_group, remaining_minutes, 
                                             last_runner_created)

        if delay: next(delay_sleeper)
        if all(j.state in END_STATES for j in job_source.jobs):
            logger.info("No jobs to process. Exiting main loop now.")
            break
    
def on_exit(runner_group, transition_pool, job_source, writer_proc):
    '''Exit cleanup'''
    global HANDLING_EXIT
    if HANDLING_EXIT: return
    HANDLING_EXIT = True

    logger.debug("Entering on_exit cleanup function")
    logger.debug("on_exit: update/remove/timeout jobs from runner group")
    runner_group.update_and_remove_finished(timeout=True)

    logger.debug("on_exit: send end message to transition threads")
    transition_pool.end_and_wait()
    
    client = db_writer.ZMQClient()
    client.term_server()
    logger.debug("on_exit: Launcher exit graceful\n\n")
    sys.exit(0)


def get_args(inputcmd=None):
    '''Parse command line arguments'''
    parser = argparse.ArgumentParser(description="Start Balsam Job Launcher.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--job-file', help="File of Balsam job IDs")
    group.add_argument('--consume-all', action='store_true', 
                        help="Continuously run all jobs from DB")
    group.add_argument('--wf-name',
                       help="Continuously run jobs of specified workflow")
    parser.add_argument('--num-workers', type=int, default=1,
                        help="Theta: defaults to # nodes. BGQ: the # of subblocks")
    parser.add_argument('--nodes-per-worker', type=int, default=1)
    parser.add_argument('--max-ranks-per-node', type=int, default=1,
                        help="For non-MPI jobs, how many to pack per worker")
    parser.add_argument('--time-limit-minutes', type=float, default=0,
                        help="Provide a walltime limit if not already imposed")
    parser.add_argument('--daemon', action='store_true')
    if inputcmd:
        return parser.parse_args(inputcmd)
    else:
        return parser.parse_args()

def detect_dead_runners(job_source):
    '''Jobs found in the RUNNING state before the Launcher starts may have
    crashed; pick them up and restart'''
    for job in job_source.by_states['RUNNING']:
        logger.info(f'Picked up dead running job {job.cute_id}: marking RESTART_READY')
        job.update_state('RESTART_READY', 'Detected dead runner')

def launch_db_writer_process():
    import multiprocessing
    INSTALL_PATH = settings.INSTALL_PATH
    path = os.path.join(INSTALL_PATH, db_writer.SOCKFILE_NAME)
    if os.path.exists(path):
        os.remove(path)

    writer_proc = multiprocessing.Process(target=db_writer.server_main)
    writer_proc.daemon = True
    writer_proc.start()
    return writer_proc

if __name__ == "__main__":
    args = get_args()
    
    writer_proc = launch_db_writer_process()
    job_source = jobreader.JobReader.from_config(args)
    job_source.refresh_from_db()
    transition_pool = transitions.TransitionProcessPool()
    runner_group  = runners.RunnerGroup(transition_pool.lock)
    worker_group = worker.WorkerGroup(args, host_type=scheduler.host_type,
                                      workers_str=scheduler.workers_str,
                                      workers_file=scheduler.workers_file)

    detect_dead_runners(job_source)

    handl = lambda a,b: on_exit(runner_group, transition_pool, job_source, writer_proc)
    signal.signal(signal.SIGINT, handl)
    signal.signal(signal.SIGTERM, handl)
    signal.signal(signal.SIGHUP, handl)

    main(args, transition_pool, runner_group, job_source)
    on_exit(runner_group, transition_pool, job_source, writer_proc)
