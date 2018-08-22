'''Main Launcher entry point

The ``main()`` function contains the Launcher service loop, in which:
    1. Transitions are checked for completion and jobs states are updated
    2. Dependencies of waiting jobs are checked
    3. New transitions are added to the transitions queue
    4. The RunnerGroup is checked for jobs that have stopped execution
    5. A new Runner is created according to logic in create_next_runner

The ``sig_handler()`` handler is invoked either when the time limit is exceeded or
if the program receives a SIGTERM or SIGINT. This takes the necessary cleanup
actions and is guaranteed to execute only once through the EXIT_FLAG global
flag.
'''
import argparse
from collections import defaultdict
from importlib.util import find_spec
from math import floor
import os
import sys
import signal
import subprocess
import time

import django
os.environ['DJANGO_SETTINGS_MODULE'] = 'balsam.django_config.settings'
django.setup()
from django.conf import settings
from django import db

import logging
logger = logging.getLogger('balsam.launcher')
logger.info("Loading Balsam Launcher")

from balsam.launcher import transitions
from balsam.launcher import worker
from balsam.launcher.util import remaining_time_minutes, delay_generator
from balsam.launcher.exceptions import *
from balsam.scripts.cli import config_launcher_subparser
from balsam.service import models
BalsamJob = models.BalsamJob

EXIT_FLAG = False

def sig_handler(signum, stack):
    global EXIT_FLAG
    EXIT_FLAG = True

class MPIRun:
    RUN_DELAY = 0.01 # 1000 jobs / 10 sec

    def __init__(self, job, workers):
        self.job = job
        self.workers = workers
        for w in self.workers: w.idle = False

        envs = job.get_envs() # dict
        app_cmd = job.app_cmd
        nranks = job.num_ranks
        affinity = job.cpu_affinity
        rpn = job.ranks_per_node
        tpr = job.threads_per_rank
        tpc = job.threads_per_core

        mpi_cmd = workers[0].mpi_cmd
        mpi_str = mpi_cmd(workers, app_cmd=app_cmd, envs=envs,
                               num_ranks=nranks, ranks_per_node=rpn,
                               cpu_affinity=affinity, threads_per_rank=tpr, threads_per_core=tpc)
        basename = job.name
        outname = os.path.join(job.working_directory, f"{basename}.out")
        logger.info(f"{job.cute_id} running:\n{mpi_str} on workers {workers}")
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
        time.sleep(self.RUN_DELAY)

    def free_workers(self):
        for worker in self.workers:
            worker.idle = True

class MPILauncher:
    MAX_CONCURRENT_RUNS = settings.MAX_CONCURRENT_MPIRUNS

    def __init__(self, wf_name, fork_rpn, time_limit_minutes, gpus_per_node):
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
        os.environ['BALSAM_LAUNCHER_NODES'] = str(self.total_nodes)
        os.environ['BALSAM_SERIAL_RPN'] = "0"

        self.timer = remaining_time_minutes(time_limit_minutes)
        self.delayer = delay_generator()
        self.last_report = 0
        self.exit_counter = 0
        self.mpi_runs = []

    def time_step(self):
        '''Pretty log of remaining time'''
        global EXIT_FLAG

        next(self.delayer)
        try:
            minutes_left = next(self.timer)
        except StopIteration:
            EXIT_FLAG = True
            return

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
        processable_count = manager.by_states(models.PROCESSABLE_STATES).count()
        if processable_count > 0: 
            return
        if self.get_runnable().count() > 0:
            self.exit_counter = 0
            return
        else: 
            self.exit_counter += 1
        if self.exit_counter == 10:
            EXIT_FLAG = True

    def check_state(self, run):
        retcode = run.process.poll()
        if retcode is None:
            run.current_state = 'RUNNING'
        elif retcode == 0:
            logger.info(f"MPIRun {run.job.cute_id} done")
            run.current_state = 'RUN_DONE'
            run.outfile.close()
            run.free_workers()
        else:
            run.process.communicate()
            run.outfile.close()
            tail = get_tail(run.outfile.name)
            run.current_state = 'RUN_ERROR'
            run.err_msg = tail
            logger.info("MPIRun {run.job.cute_id} error code {retcode}:\n{tail}")
            run.free_workers()
        return run.current_state

    def timeout_kill(self, runs, timeout=10):
        for run in runs: run.process.terminate() #SIGTERM
        start = time.time()
        for run in runs:
            try: 
                run.process.wait(timeout=timeout)
            except: 
                break
            if time.time() - start > timeout: break
        for run in runs: 
            run.process.kill()
            run.free_workers()

    def update(self, timeout=False):
        by_states = defaultdict(list)
        for run in self.mpi_runs:
            state = self.check_state(run)
            by_states[state].append(run)

        done_pks = [r.job.pk for r in by_states['RUN_DONE']]
        BalsamJob.batch_update_state(done_pks, 'RUN_DONE')
        self.jobsource.release(done_pks)
        
        error_pks = [r.job.pk for r in by_states['RUN_ERROR']]
        with db.transaction.atomic():
            for run in by_states['RUN_ERROR']:
                run.job.update_state('RUN_ERROR', run.err_msg)
        self.jobsource.release(error_pks)
        
        active_pks = [r.job.pk for r in by_states['RUNNING']]
        if timeout: 
            self.timeout_kill(by_states['RUNNING'])
            BalsamJob.batch_update_state(active_pks, 'RUN_TIMEOUT')
            self.jobsource.release(active_pks)
        else:
            killquery = self.jobsource.filter(job_id__in=active_pks, state='USER_KILLED')
            kill_pks  = killquery.values_list('job_id', flat=True)
            to_kill = [run for run in by_states['RUNNING'] if run.job.pk in kill_pks]
            self.timeout_kill(to_kill)
            self.jobsource.release(kill_pks)
            for run in to_kill: by_states['RUNNING'].remove(run)

        if timeout:
            self.mpi_runs = []
        else:
            self.mpi_runs = by_states['RUNNING']
        
    def get_runnable(self):
        '''queryset: jobs that can finish on currently idle workers within time limits'''
        remaining_minutes = next(self.timer)
        manager = self.jobsource
        num_idle = len(self.worker_group.idle_workers())
        if num_idle == 0:
            logger.info(f'No idle worker nodes to run jobs')
            return manager.none()
        else:
            logger.info(f'{num_idle} idle worker nodes')

        return manager.get_runnable(max_nodes=num_idle,
                                    remaining_minutes=remaining_minutes,
                                    order_by='-num_nodes')

    def report_constrained(self):
        now = time.time()
        elapsed = now - self.last_report
        if elapsed < 10:
            return
        else:
            self.last_report = now

        all_runnable = BalsamJob.objects.filter(state__in=models.RUNNABLE_STATES)
        unlocked = all_runnable.filter(lock='')
        logger.info(f'{all_runnable.count()} runnable jobs across entire Balsam DB')
        logger.info(f'{unlocked.count()} of these are unlocked')
        if self.jobsource.workflow:
            wf_match = unlocked.filter(workflow=self.jobsource.workflow)
            logger.info(f'{wf_match.count()} of these match the current workflow filter')
        logger.info(f'If nothing is running; the job node requirements or walltime '
                    'requirements may exceed the available nodes or remaining time')

    def launch(self):
        num_idle = len(self.worker_group.idle_workers())
        num_active = len(self.mpi_runs)
        max_acquire = min(num_idle, self.MAX_CONCURRENT_RUNS - num_active)
        max_acquire = max(max_acquire, 0)

        if num_active >= self.MAX_CONCURRENT_RUNS:
            logger.info(f'Reached MAX_CONCURRENT_MPIRUNS limit')
            return

        runnable = self.get_runnable()
        num_runnable = runnable.count()
        if num_runnable > 0:
            logger.debug(f"{num_runnable} runnable jobs")
        else:
            logger.info('No runnable jobs')
            self.report_constrained()
            return

        # pre-assign jobs to nodes (descending order of node count)
        cache = list(runnable)
        pre_assignments = []
        for job in cache:
            workers = self.worker_group.request(job.num_nodes)
            if workers:
                pre_assignments.append((job, workers))
            else:
                assert job.num_nodes > sum(w.num_nodes for w in self.worker_group.idle_workers())
                break

        # acquire lock on jobs
        to_acquire = [job.pk for (job,workers) in pre_assignments]
        acquired_pks = self.jobsource.acquire(to_acquire).values_list('job_id', flat=True)
        logger.info(f'Acquired lock on {len(acquired_pks)} out of {len(pre_assignments)} jobs marked for running')

        # dispatch runners; release workers that did not acquire job
        for (job, workers) in pre_assignments:
            if job.pk in acquired_pks:
                run = MPIRun(job, workers)
                self.mpi_runs.append(run)
            else:
                for w in workers: w.idle = True
        if self.jobsource.qLaunch is not None:
            sched_id = self.jobsource.qLaunch.scheduler_id
            message = f'Batch Scheduler ID: {sched_id}'
        else:
            message = 'Not scheduled by service'
        BalsamJob.batch_update_state(acquired_pks, 'RUNNING', message=message)

    def run(self):
        '''Main Launcher service loop'''
        global EXIT_FLAG
        try:
            while not EXIT_FLAG:
                self.time_step()
                self.launch()
                self.update()
                self.check_exit()
        except:
            raise
        finally:
            logger.info('EXIT: breaking launcher service loop')
            self.update(timeout=True)
            assert not self.is_active
            logger.info('All MPI runs terminated')
            self.jobsource.release_all_owned()
            logger.info('Released all BalsamJob locks')

    @property
    def is_active(self):
        return len(self.mpi_runs) > 0


class SerialLauncher:
    MPI_ENSEMBLE_EXE = find_spec("balsam.launcher.mpi_ensemble").origin

    def __init__(self, wf_name=None, fork_rpn=1, time_limit_minutes=60, gpus_per_node=None):
        self.wf_name = wf_name
        self.fork_rpn = fork_rpn
        self.gpus_per_node = gpus_per_node

        timer = remaining_time_minutes(time_limit_minutes)
        minutes_left = max(0.1, next(timer) - 1)
        self.worker_group = worker.WorkerGroup()
        self.total_nodes = sum(w.num_nodes for w in self.worker_group)
        os.environ['BALSAM_LAUNCHER_NODES'] = str(self.total_nodes)
        os.environ['BALSAM_SERIAL_RPN'] = str(self.fork_rpn)

        self.app_cmd = f"{sys.executable} {self.MPI_ENSEMBLE_EXE}"
        self.app_cmd += f" --time-limit-min={minutes_left}"
        if self.wf_name: self.app_cmd += f" --wf-name={self.wf_name}"
        if self.gpus_per_node: self.app_cmd += f" --gpus-per-node={self.gpus_per_node}"
    
    def run(self):
        global EXIT_FLAG
        workers = self.worker_group
        num_ranks = self.total_nodes * self.fork_rpn
        mpi_str = workers.mpi_cmd(workers, app_cmd=self.app_cmd,
                               num_ranks=num_ranks, ranks_per_node=self.fork_rpn,
                               cpu_affinity='none', envs={})
        logger.info(f'Starting MPI Fork ensemble process:\n{mpi_str}')

        self.outfile = open(os.path.join(settings.LOGGING_DIRECTORY, 'ensemble.out'), 'wb')
        self.process = subprocess.Popen(
            args=mpi_str.split(),
            bufsize=1,
            stdout=self.outfile,
            stderr=subprocess.STDOUT,
            shell=False)

        while not EXIT_FLAG:
            try: 
                retcode = self.process.wait(timeout=2)
            except subprocess.TimeoutExpired: 
                pass
            else:
                logger.info(f'ensemble pull subprocess returned {retcode}')
                break

        self.process.terminate()
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.process.kill()
        self.outfile.close()
        
def main(args):
    signal.signal(signal.SIGINT,  sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    wf_filter = args.wf_filter
    job_mode = args.job_mode
    timelimit_min = args.time_limit_minutes
    nthread = (args.num_transition_threads if args.num_transition_threads
              else settings.NUM_TRANSITION_THREADS)
    fork_rpn = (args.serial_jobs_per_node if args.serial_jobs_per_node
              else settings.SERIAL_JOBS_PER_NODE)
    gpus_per_node = args.gpus_per_node
    
    Launcher = MPILauncher if job_mode == 'mpi' else SerialLauncher
    
    try:
        transition_pool = transitions.TransitionProcessPool(nthread, wf_filter)
        launcher = Launcher(wf_filter, fork_rpn, timelimit_min, gpus_per_node)
        launcher.run()
    except:
        raise
    finally:
        transition_pool.terminate()
        logger.debug("Launcher exit graceful\n\n")

def get_args(inputcmd=None):
    '''Parse command line arguments'''
    parser = config_launcher_subparser()
    if inputcmd:
        return parser.parse_args(inputcmd)
    else:
        return parser.parse_args()

if __name__ == "__main__":
    args = get_args()
    main(args)
