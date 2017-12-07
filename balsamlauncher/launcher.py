'''The Launcher is either invoked by the user, who bypasses the Balsam
scheduling service and submits directly to a local job queue, or by the
Balsam service metascheduler'''
import argparse
import os
from sys import exit
import signal
import time

import django
os.environ['DJANGO_SETTINGS_MODULE'] = 'argobalsam.settings'
django.setup()
from django.conf import settings

import logging
logger = logging.getLogger('balsamlauncher')
logger.info("Loading Balsam Launcher")

from balsam.schedulers import Scheduler
scheduler = Scheduler.scheduler_main

from balsamlauncher import jobreader
from balsamlauncher import transitions
from balsamlauncher import worker
from balsamlauncher import runners
from balsamlauncher.exceptions import *

RUNNABLE_STATES = ['PREPROCESSED', 'RESTART_READY']

def delay(period=settings.BALSAM_SERVICE_PERIOD):
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
    start = time.time()
    while True:
        yield (time.time() - start) / 60.0

def sufficient_time(job):
    return 60*job.wall_time_minutes < scheduler.remaining_time_seconds()

def get_runnable_jobs(jobs, running_pks):
    runnable_jobs = [job for job in jobs
                     if job.pk not in running_pks and
                     job.state in RUNNABLE_STATES and
                     sufficient_time(job)]
    return runnable_jobs

def create_new_runners(jobs, runner_group, worker_group):
    created_one = False
    running_pks = runner_group.running_job_pks
    runnable_jobs = get_runnable_jobs(jobs, running_pks)
    while runnable_jobs:
        logger.debug(f"Have {len(runnable_jobs)} new runnable jobs (out of "
                     f"{len(jobs)})")
        try:
            runner_group.create_next_runner(runnable_jobs, worker_group)
        except ExceededMaxRunners:
            logger.info("Exceeded max concurrent runners; waiting")
            break
        except NoAvailableWorkers:
            logger.info("Not enough idle workers to start any new runs")
            break
        else:
            created_one = True
            running_pks = runner_group.running_job_pks
            runnable_jobs = get_runnable_jobs(jobs, running_pks)
    return created_one

def main(args, transition_pool, runner_group, job_source):
    delay_timer = delay()
    if args.time_limit_minutes > 0:
        timeout = lambda : elapsed_time_minutes() >= args.time_limit_minutes
    else:
        timeout = lambda : scheduler.remaining_time_seconds() <= 0.0

    while not timeout():
        logger.debug("\n******************\n"
                       "BEGIN SERVICE LOOP\n"
                       "******************")
        wait = True

        for stat in transition_pool.get_statuses():
            wait = False
        
        job_source.refresh_from_db()
        
        transitionable_jobs = [
            job for job in job_source.jobs
            if job not in transition_pool
            and job.state in transitions.TRANSITIONS
        ]
        for job in transitionable_jobs:
            transition_pool.add_job(job)
            wait = False
            fxn = transitions.TRANSITIONS[job.state]
            logger.info(f"Queued transition: {job.cute_id} will undergo {fxn}")
        
        any_finished = runner_group.update_and_remove_finished()
        job_source.refresh_from_db()
        created = create_new_runners(job_source.jobs, runner_group, worker_group)
        if any_finished or created: wait = False
        if wait: next(delay_timer)
    
def on_exit(runner_group, transition_pool, job_source):
    transition_pool.flush_job_queue()

    runner_group.update_and_remove_finished()
    for runner in runner_group:
        runner.timeout()

    job_source.refresh_from_db()
    timedout_jobs = job_source.by_states['RUN_TIMEOUT']
    for job in timedout_jobs:
        transition_pool.add_job(job)

    transition_pool.end_and_wait()
    logger.debug("Launcher exit graceful\n\n")
    exit(0)


def get_args(inputcmd=None):
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
    parser.add_argument('--time-limit-minutes', type=int, default=0,
                        help="Provide a walltime limit if not already imposed")
    parser.add_argument('--daemon', action='store_true')
    if inputcmd:
        return parser.parse_args(inputcmd)
    else:
        return parser.parse_args()

def detect_dead_runners(job_source):
    for job in job_source.by_states['RUNNING']:
        logger.info(f'Picked up running job {job.cute_id}: marking RESTART_READY')
        job.update_state('RESTART_READY', 'Detected dead runner')

if __name__ == "__main__":
    args = get_args()
    
    job_source = jobreader.JobReader.from_config(args)
    job_source.refresh_from_db()
    transition_pool = transitions.TransitionProcessPool()
    runner_group  = runners.RunnerGroup(transition_pool.lock)
    worker_group = worker.WorkerGroup(args, host_type=scheduler.host_type,
                                      workers_str=scheduler.workers_str,
                                      workers_file=scheduler.workers_file)

    detect_dead_runners(job_source)

    handl = lambda a,b: on_exit(runner_group, transition_pool, job_source)
    signal.signal(signal.SIGINT, handl)
    signal.signal(signal.SIGTERM, handl)
    signal.signal(signal.SIGHUP, handl)

    main(args, transition_pool, runner_group, job_source)
    on_exit(runner_group, transition_pool, job_source)
