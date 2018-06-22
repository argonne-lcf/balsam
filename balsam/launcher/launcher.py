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
    @classmethod
    def init_launcher(cls, wf_name, job_mode, fork_rpn, time_limit_minutes):
        if job_mode == 'mpi':
            return MPILauncher(wf_name, time_limit_minutes)
        elif job_mode == 'serial':
            return SerialLauncher(wf_name, fork_rpn, time_limit_minutes)

    def __init__(self, wf_name, time_limit_minutes):
        self.jobsource = BalsamJob.source
        self.jobsource.workflow = wf_name
        if wf_name:
            logger.info(f'Filtering jobs with workflow matching {wf_name}')
        else:
            logger.info('Consuming all jobs from DB')

        self.jobsource.clear_stale_locks()
        self.jobsource.start_tick()
        self.worker_group = worker.WorkerGroup()
        self.total_nodes = sum(w.num_nodes for w in self.worker_group)

        self.timer = remaining_time_minutes(time_limit_minutes)
        self.delayer = delay_generator()

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

    def check_exit(self):
        global EXIT_FLAG

        remaining_minutes = next(self.timer)
        if remaining_minutes <= 0:
            EXIT_FLAG = True
            return
        if self.is_active: 
            return
        manager = self.jobsource
        processable_count = manager.by_states(PROCESSABLE_STATES).count()
        if processable_count > 0: 
            return
        if self.get_runnable().count() > 0:
            self.exit_counter = 0
            return
        else: 
            self.exit_counter += 1
        if self.exit_counter == 10:
            EXIT_FLAG = True

class MPILauncher(Launcher):
    MAX_CONCURRENT_RUNS = settings.MAX_CONCURRENT_MPIRUNS
    class MPIRun:
        def __init__(self, job, workers):
            self.job = job

            envs = job.get_envs() # dict
            app_cmd = job.app_cmd
            nranks = job.num_ranks
            affinity = job.cpu_affinity
            rpn = job.ranks_per_node
            tpr = job.threads_per_rank
            tpc = job.threads_per_core

            mpi_str = self.mpi_cmd(workers, app_cmd=app_cmd, envs=envs,
                                   num_ranks=nranks, ranks_per_node=rpn,
                                   affinity=affinity, threads_per_rank=tpr, threads_per_core=tpc)
            basename = job.name
            outname = os.path.join(job.working_directory, f"{basename}.out")
            logger.info(f"{job.cute_id} running:\n{mpi_str}")
            self.outfile = open(outname, 'w+b')
            self.process = subprocess.Popen(
                    args=mpi_str.split(),
                    cwd=job.working_directory,
                    stdout=self.outfile,
                    stderr=subprocess.STDOUT,
                    shell=False,
                    bufsize=1
                    )
            self.current_state = 'RUNNING'
            self.err_msg = None

    def __init__(self, wf_name):
        super().__init__(wf_name, time_limit_minutes)
        self.mpi_runs = []

    def check_state(self, run):
        retcode = run.process.poll()
        if retcode is None:
            run.current_state = 'RUNNING'
        elif retcode == 0:
            logger.info(f"MPIRun {run.job.cute_id} done")
            run.current_state = 'RUN_DONE'
            run.outfile.close()
        else:
            run.process.communicate()
            run.outfile.close()
            tail = get_tail(run.outfile.name)
            run.current_state = 'RUN_ERROR'
            run.err_msg = tail
            logger.info("MPIRun {run.job.cute_id} error code {retcode}:\n{tail}")

    def timeout_kill(self, runs, timeout=10):
        for run in runs: run.process.terminate() #SIGTERM
        start = time.time()
        for run in runs:
            try: 
                run.process.wait(timeout=timeout)
            except: 
                break
            if time.time() - start > timeout: break
        for run in runs: run.process.kill()

    def update(self, timeout=False):
        for run in self.mpi_runs:
            self.check_state(run)

        # done jobs
        done_pks = [run.job.pk
                    for run in self.mpi_runs
                    if run.current_state == 'RUN_DONE']
        if done_pks: BalsamJob.batch_update_state(done_pks, 'RUN_DONE')

        # error jobs
        error_runs = [run for run in self.mpi_runs
                     if run.current_state == 'RUN_ERROR']
        with db.transaction.atomic():
            for run in error_runs: run.job.update_state('RUN_ERROR', run.err_msg)
        
        # timedout or killed
        active_runs = [run for run in self.mpi_runs if run.current_state == 'RUNNING']
        active_pks = [r.job.pk for r in active_runs]
        if timeout:
            timedout_pks = active_pks
            self.timeout_kill(active_runs)
            if active_pks: BalsamJob.batch_update_state(active_pks, 'RUN_TIMEOUT')
        else:
            timedout_pks = []
            killed_pks = self.jobsource.filter(pk__in=active_pks,
                    state='USER_KILLED').values_list('job_id', flat=True)
            killed_runs = [run for run in self.mpi_runs if run.job.pk in killed_pks]
            self.timeout_kill(killed_runs)

        # For finished runners, free workers; remove from list
        error_pks = [r.job.pk for r in error_runs]
        finished_pks = list(chain(done_pks, error_pks, timedout_pks, killed_pks))
        self.mpi_runs = [r for r in self.mpi_runs if r.job.pk not in finished_pks]
        

    def get_runnable(self):
        manager = self.jobsource
        num_idle = len(self.worker_group.idle_workers())
        max_workers = len(self.worker_group) - self.ensemble_nodes
        num_mpi_nodes = min(num_idle, max_workers)
        runnable = manager.get_runnable(max_nodes=total_nodes, mpi_only=True,
                                        time_limit_minutes=remaining_minutes)

    def launch(self):
        if len(self.mpi_runs) == self.MAX_CONCURRENT_RUNS:
            logger.info(f'Reached MAX_CONCURRENT_MPIRUNS limit')
            return

        # get runnable
        runnable = self.get_runnable()
        num_runnable = runnable.count()
        if num_runnable > 0:
            logger.debug(f"Have {runnable_jobs.count()} runnable jobs")
        else:
            logger.info('No runnable jobs')
            return

        # create list of jobs to run; pre-assign workers
        # order by node count..favor large to small
        # go ahead and run non-MPI jobs (but last..as aprun -n 1)

        # acquire lock on jobs

        # dispatch runners; add to list

        # batch-update RUNNING

        # release workers that did not acquire the job

    @property
    def is_active(self):
        return len(self.mpi_runs) > 0


class SerialLauncher(Launcher):
    def __init__(self, wf_name, fork_rpn, time_limit_minutes):
        super().__init__(wf_name, time_limit_minutes)
        self.ensemble_runner = None
    
    def update(self, timeout=False):
        pass

    @property
    def is_active(self):
        try:
            return self.ensemble_runner.poll() is None
        except AttributeError:
            return False
    
    def get_runnable(self):
        pass
    
    def launch(self):
        runnable = self.get_runnable()
        logger.debug(f"Have {runnable_jobs.count()} runnable jobs")


def _main(launcher):
    '''Main Launcher service loop'''
    global EXIT_FLAG

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

    wf_filter = args.wf_filter
    job_mode = args.job_mode
    timelimit_min = args.time_limit_minutes
    nthread = args.num_transition_threads if args.num_transition threads
              else settings.BALSAM_MAX_CONCURRENT_TRANSITIONS
    fork_rpn = args.serial_jobs_per_node if args.serial_jobs_per_node
              else settings.SERIAL_JOBS_PER_NODE
    
    try:
        launcher = Launcher.init_launcher(wf_filter, job_mode, fork_rpn, timelimit_min)
        transition_pool = transitions.TransitionProcessPool(nthread)
        _main(launcher)
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
    group.add_argument('--wf-filter', help="Continuously run jobs of specified workflow")
    parser.add_argument('--job-mode', choices=['mpi', 'serial'],
            required=True, default='mpi')
    parser.add_argument('--serial-jobs-per-node', type=int, default=None, 
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
