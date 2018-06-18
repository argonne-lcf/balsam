'''Main Launcher entry point

The ``main()`` function contains the Launcher service loop, in which:
    1. Transitions are checked for completion and jobs states are updated
    2. Dependencies of waiting jobs are checked
    3. New transitions are added to the transitions queue
    4. The RunnerGroup is checked for jobs that have stopped execution
    5. A new Runner is created according to logic in create_next_runner

The ``on_exit()`` handler is invoked either when the time limit is exceeded or
if the program receives a SIGTERM or SIGINT. This takes the necessary cleanup
actions and is guaranteed to execute only once through the EXIT_FLAG global
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
scheduler = Scheduler.scheduler_main

from balsam.launcher import transitions
from balsam.launcher import worker
from balsam.launcher import runners
from balsam.launcher.util import remaining_time_minutes, delay_generator
from balsam.launcher.exceptions import *

EXIT_FLAG = False

def on_exit(signum, stack):
    global EXIT_FLAG
    EXIT_FLAG = True


class Launcher:

    def __init__(self, wf_name, ensemble_nodes, ensemble_rpn, time_limit_minutes):
        self.jobsource = BalsamJob.source
        self.jobsource.workflow = wf_name
        if wf_name:
            logger.info(f'Filtering jobs with workflow matching {wf_name}')
        else:
            logger.info('Consuming all jobs from DB')

        self.jobsource.clear_stale_locks()
        self.jobsource.start_tick()
        self.mpi_jobs = []
        self.worker_group = worker.WorkerGroup()

        if self.ensemble_nodes
        self.serial_ensemble = EnsembleRunner(wf_name, ensemble_nodes, ensemble_rpn,
                                              time_limit_minutes)

        self.total_nodes = sum(w.num_nodes for w in self.worker_group)
        self.ensemble_nodes = ensemble_nodes
        assert self.ensemble_nodes <= self.total_nodes

        self.timer = remaining_time_minutes(time_limit_minutes)
        self.delayer = delay_generator()

    def get_runnable(self):
        manager = self.jobsource
        num_idle = len(self.worker_group.idle_workers())
        max_workers = len(self.worker_group) - self.ensemble_nodes
        num_mpi_nodes = min(num_idle, max_workers)
        runnable = manager.get_runnable(max_nodes=total_nodes, mpi_only=True,
                                        time_limit_minutes=remaining_minutes)

    def time_step(self):
        '''Pretty log of remaining time'''
        next(self.delayer)
        minutes_left = next(self.timer)
        if minutes_left > 1e12:
            return
        whole_minutes = floor(minutes_left)
        whole_seconds = round((minutes_left - whole_minutes)*60)
        time_str = f"{whole_minutes:02d} min : {whole_seconds:02d} sec remaining"
        logger.info(time_str)

    def launch_mpi(self):
        '''Decide whether or not to create another runner. Considers how many jobs
        can run, how many can *almost* run, how long since the last Runner was
        created, and how many jobs are serial as opposed to MPI.
        '''
        jobsource = self.jobsource
        runnable = jobsource.get_runnable(remaining_minutes)
        runnable_jobs = runnable_jobs.exclude(job_pk__in=runner_group.running_job_pks)
        logger.debug(f"Have {runnable_jobs.count()} runnable jobs")

        # If nothing is getting pre-processed, don't bother waiting
        almost_runnable = job_source.by_states(job_source.ALMOST_RUNNABLE_STATES).exists()

        # If it has been runner_create_period seconds, don't wait any more
        runner_create_period = settings.BALSAM_RUNNER_CREATION_PERIOD_SEC
        now = time.time()
        runner_ready = bool(now - last_runner_created > runner_create_period)

        # If there are enough serial jobs, don't wait to run
        num_serial = runnable_jobs.filter(num_nodes=1).filter(ranks_per_node=1).count()
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

    def check_exit(self):
        global EXIT_FLAG

        remaining_minutes = next(self.timer)
        if remaining_minutes <= 0:
            EXIT_FLAG = True
            return
        
        if self.serial_ensemble.is_active(): return
        
        manager = self.jobsource
        processable_count = manager.by_states(PROCESSABLE_STATES).count()
        if processable_count > 0: return
        active_count = manager.by_states(ACTIVE_STATES).count()
        if active_count > 0: return


        # this should take account of how many nodes are allocated for MPI jobs
        total_nodes = sum(w.num_nodes for w in self.worker_group) # FIX THIS
        runnable = manager.get_runnable(max_nodes=total_nodes, mpi_only=True,
                                        time_limit_minutes=remaining_minutes)
        if runnable.count() > 0:
            return
        else:
            EXIT_FLAG = True

    def update_runners(self, timeout=False):
        pass


def _main(args, worker_group):
    '''Main Launcher service loop'''
    global EXIT_FLAG
    delay_sleeper = delay_generator()
    last_runner_created = time.time()
    exit_counter = 0
    launcher = Launcher()

    while not EXIT_FLAG:
        launcher.time_step()

        # Update jobs that are running/finished
        launcher.update_runners()
        any_finished = runner_group.update_and_remove_finished()
        if any_finished: delay = False
    
        # Decide whether or not to start a new runner
        last_runner_created = create_runner(runner_group, 
                                             worker_group, remaining_minutes, 
                                             last_runner_created)

        check_exit()

    if EXIT_FLAG:
        logger.info('EXIT: breaking launcher service loop')
    launcher.update_runners(timeout=True)


def main(args):
    signal.signal(signal.SIGINT,  on_exit)
    signal.signal(signal.SIGTERM, on_exit)

    wf_filter = args.consume_workflow
    num_ensemble_nodes = args.ensemble_nodes
    timelimit_min = args.time_limit_minutes
    nthread = args.num_transition_threads if args.num_transition threads
              else settings.BALSAM_MAX_CONCURRENT_TRANSITIONS
    ensemble_rpn = args.ensemble_ranks_per_node if args.ensemble_ranks_per_node
              else settings.ENSEMBLE_RANKS_PER_NODE
    
    launcher = Launcher(wf_filter, num_ensemble_nodes, ensemble_rpn, timelimit_min)
    
    try:
        transition_pool = transitions.TransitionProcessPool(nthread)
        _main(args, worker_group)
    except:
        raise
    finally:
        transition_pool.terminate()
        manager.release_all_owned()
        logger.debug("Launcher exit graceful\n\n")

def get_args(inputcmd=None):
    '''Parse command line arguments'''
    parser = argparse.ArgumentParser(description="Start Balsam Job Launcher.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--consume-all', action='store_true', help="Continuously run all jobs from DB")
    group.add_argument('--consume-workflow', help="Continuously run jobs of specified workflow")
    parser.add_argument('--ensemble-nodes', type=int, default=None)
    parser.add_argument('--ensemble-ranks-per-node', type=int, default=None, 
                        help="Single-node jobs: max-per-node")
    parser.add_argument('--time-limit-minutes', type=float, default=0, 
                        help="Provide a walltime limit if not already imposed")
    parser.add_argument('--num-transition-threads', type=int, default=None)
    if inputcmd:
        return parser.parse_args(inputcmd)
    else:
        return parser.parse_args()

if __name__ == "__main__":
    args = get_args()
    main(args)
