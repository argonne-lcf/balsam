'''The Launcher is either invoked by the user, who bypasses the Balsam
scheduling service and submits directly to a local job queue, or by the
Balsam service metascheduler'''
import argparse
from collections import defaultdict
import os
import multiprocessing
import queue
import time

from django.conf import settings
from django.db import transaction

from balsam import scheduler
from balsam.launcher import jobreader
from balsam.launcher import transitions
from balsam.launcher import worker
from balsam.launcher.exceptions import *

START_TIME = time.time() + 10.0

SIGTIMEOUT = 'TIMEOUT'
SIGNALS = {
    signal.SIGINT: 'SIG_INT',
    signal.SIGTERM: 'SIG_TERM',
}

def delay(period=10.0):
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

class HostEnvironment:
    '''Set user- and environment-specific settings for this run'''
    RECOGNIZED_HOSTS = {
        'BGQ'  : 'vesta cetus mira'.split(),
        'CRAY' : 'theta'.split(),
    }

    def __init__(self, args):
        self.hostname = None
        self.host_type = None
        self.scheduler_id = None
        self.pid = None
        self.num_nodes = None
        self.partition = None
        self.walltime_seconds = None

        self.job_file = args.job_file
        self.wf_name = args.consume_wf
        self.consume_all = args.consume_all
        self.num_workers = args.num_workers
        self.ranks_per_worker_serial = args.serial_jobs_per_worker
        self.walltime_minutes = args.time_limit_minutes
        
        self.set_hostname_and_type()
        self.query_scheduler()
        
        if self.walltime_minutes is not None:
            self.walltime_seconds = self.walltime_minutes * 60


    def set_hostname_and_type(self):
        from socket import gethostname
        self.pid = os.getpid()
        hostname = gethostname().lower()
        self.hostname = hostname
        for host_type, known_names in RECOGNIZED_HOSTS.values():
            if any(hostname.find(name) >= 0 for name in known_names):
                self.host_type = host_type
                return
        self.host_type = 'DEFAULT'

    def query_scheduler(self):
        if not scheduler.scheduler_class: return
        env = scheduler.get_environ()
        self.scheduler_id = env.id
        self.num_nodes = env.num_nodes
        self.partition = env.partition
        
        info = scheduler.get_job_status(self.scheduler_id)
        self.walltime_seconds = info['walltime_sec']

    def elapsed_time_seconds(self):
        return time.time() - START_TIME

    def remaining_time_seconds(self):
        if self.walltime_seconds:
            elasped = self.elapsed_time_seconds()
            return self.walltime_seconds - elapsed
        else:
            return float("inf")

    def sufficient_time(self, job):
        return 60*job.wall_time_minutes < self.remaining_time_seconds()

    def check_timeout(self):
        if self.remaining_time_seconds() < 1.0:
            return True
        return False

def get_runnable_jobs(jobs, running_pks, host_env):
    runnable_jobs = [job for job in jobsource.jobs
                     if job.pk not in running_pks and
                     job.state in RUNNABLE_STATES and
                     host_env.sufficient_time(job)]
    return runnable_jobs

def create_new_runners(jobs, runner_group, worker_group, host_env):
    running_pks = runner_group.running_job_pks
    runnable_jobs = get_runnable_jobs(jobs, running_pks, host_env)
    while runnable_jobs:
        try:
            runner_group.create_next_runner(runnable_jobs, worker_group)
        except (ExceededMaxRunners, NoAvailableWorkers) as e:
            break
        else:
            running_pks = runner_group.running_job_pks
            runnable_jobs = get_runnable_jobs(jobs, running_pks, host_env)

def main(args):
    host_env = HostEnvironment(args)
    worker_group = worker.WorkerGroup(host_env)
    jobsource = jobreader.JobReader.from_config(args)

    transition_pool = transitions.TransitionProcessPool()
    runner_group  = runners.RunnerGroup()
    delay_timer = delay()

    # Main Launcher Service Loop
    while not host_env.check_timeout():
        wait = True
        for stat in transitions_pool.get_statuses():
            logger.debug(f'Transition: {stat.pk} {stat.state}: {stat.msg}')
            wait = False
        
        jobsource.refresh_from_db()
        
        transitionable_jobs = [
            job for job in jobsource.jobs
            if job not in transitions_pool
            and job.state in transitions_pool.TRANSITIONS
        ]
        for job in transitionable_jobs:
            transitions_pool.add_job(job)
            wait = False
        
        runner_group.update_and_remove_finished()
        any_finished = create_new_runners(
            jobsource.jobs, runner_group, worker_group, host_env
        )
        if any_finished: wait = False
        if wait: next(delay_timer)
    
    transitions_pool.stop_processes()
    for runner in runner_group:
        runner.timeout(SIGTIMEOUT, None)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start Balsam Job Launcher.")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--job-file', help="File of Balsam job IDs")
    group.add_argument('--consume-all', action='store_true', 
                        help="Continuously run all jobs from DB")
    group.add_argument('--wf-name',
                       help="Continuously run jobs of specified workflow")

    parser.add_argument('--num-workers', type=int, default=1,
                        help="Theta: defaults to # nodes. BGQ: the # of subblocks")
    parser.add_argument('--serial-jobs-per-worker', type=int, default=4,
                        help="For non-MPI jobs, how many to pack per worker")
    parser.add_argument('--time-limit-minutes', type=int,
                        help="Override auto-detected walltime limit (runs
                        forever if no limit is detected or specified)")
    args = parser.parse_args()
    # TODO: intercept KeyboardInterrupt and all INT,TERM signals
    # Cleanup actions; mark jobs as idle
    main(args)
