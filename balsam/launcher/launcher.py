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
from collections import defaultdict
from importlib.util import find_spec
import logging
from math import floor
from datetime import datetime
import os
import sys
import signal
import subprocess
import shlex
import time

from django import db
from balsam import config_logging, settings, setup
from balsam.core import transitions
from balsam.launcher import worker
from balsam.launcher.util import (
    remaining_time_minutes, delay_generator, get_tail
    )
from balsam.service.schedulers import JobEnv
from balsam.scripts.cli import config_launcher_subparser
from balsam.core import models

logger = logging.getLogger('balsam.launcher.launcher')
BalsamJob = models.BalsamJob
EXIT_FLAG = False


def sig_handler(signum, stack):
    global EXIT_FLAG
    EXIT_FLAG = True


class MPIRun:
    RUN_DELAY = 0.10  # 1000 jobs / 100 sec

    # TODO(KGF): extend to encapsulate scheduler-dependent quirks of starting/signaling/etc.
    # an application (e.g. do not directly kill app in Slurm; use scancel).
    # The goal is to encapsulate every platform-specific detail in a separate cluster
    # interface class, so that launcher.py can be written as generically as possible
    #
    # mpirun.start() # launch the process
    # mpirun.poll()  # check status
    # mpirun.terminate() # send friendly term signal
    # mpirun.force_kill()  # send force-kill signal
    def __init__(self, job, workers):
        self.job = job
        self.workers = workers
        for w in self.workers:
            w.idle = False

        envs = os.environ.copy()
        envs.update(job.get_envs())
        app_cmd = job.app_cmd
        nranks = job.num_ranks
        affinity = job.cpu_affinity
        rpn = job.ranks_per_node
        tpr = job.threads_per_rank
        tpc = job.threads_per_core
        mpi_flags = job.mpi_flags

        mpi_cmd = workers[0].mpi_cmd
        mpi_str = mpi_cmd(workers, app_cmd=app_cmd, envs={},
                          num_ranks=nranks, ranks_per_node=rpn,
                          cpu_affinity=affinity, threads_per_rank=tpr,
                          threads_per_core=tpc, mpi_flags=mpi_flags)
        basename = job.name
        outname = os.path.join(job.working_directory, f"{basename}.out")
        self.outfile = open(outname, 'w+b')
        envscript = job.envscript
        if envscript:
            args = ' '.join(['source', envscript, '&&', mpi_str])
            shell = True
        else:
            args = shlex.split(mpi_str)
            shell = False
        logger.info(f"{job.cute_id} Popen (shell={shell}):\n{args}\n on workers: {workers}")
        self.process = subprocess.Popen(
                args=args,
                cwd=job.working_directory,
                stdout=self.outfile,
                stderr=subprocess.STDOUT,
                shell=shell,
                env=envs,
                )
        self.current_state = 'RUNNING'
        self.err_msg = None
        time.sleep(self.RUN_DELAY)

    def free_workers(self):
        for w in self.workers:
            w.idle = True


class MPILauncher:
    MAX_CONCURRENT_RUNS = settings.MAX_CONCURRENT_MPIRUNS

    def __init__(self, wf_name, time_limit_minutes, gpus_per_node, persistent,
                 limit_nodes=None, offset_nodes=None):
        self.jobsource = BalsamJob.source
        self.jobsource.workflow = wf_name
        if wf_name:
            logger.info(f'Filtering jobs with workflow matching {wf_name}')
        else:
            logger.info('No workflow filter')

        self.jobsource.clear_stale_locks()
        self.jobsource.start_tick()
        self.worker_group = worker.WorkerGroup(limit=limit_nodes, offset=offset_nodes)
        self.total_nodes = sum(w.num_nodes for w in self.worker_group)
        os.environ['BALSAM_LAUNCHER_NODES'] = str(self.total_nodes)
        os.environ['BALSAM_JOB_MODE'] = "mpi"

        self.timer = remaining_time_minutes(time_limit_minutes)
        self.is_persistent = persistent
        self.delayer = delay_generator()
        self.last_report = 0
        self.exit_counter = 0
        self.mpi_runs = []
        self.jobsource.check_qLaunch()
        if self.jobsource.qLaunch is not None:
            sched_id = self.jobsource.qLaunch.scheduler_id
            self.RUN_MESSAGE = f'Batch Scheduler ID: {sched_id}'
        else:
            self.RUN_MESSAGE = 'Not scheduled by service'

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
        try:
            remaining_minutes = next(self.timer)
        except StopIteration:
            EXIT_FLAG = True
            logger.info("Out of time; preparing to exit")
            return
        if remaining_minutes <= 0:
            EXIT_FLAG = True
            logger.info("Out of time; preparing to exit")
            return
        if not self.is_persistent:
            if self.is_active:
                # Reset exit counter whenever jobs are running, runable, or transitionble
                self.exit_counter = 0
                logger.debug("Some runs are still active; will not quit")
                return
            processable = BalsamJob.objects.filter(state__in=models.PROCESSABLE_STATES)
            if self.jobsource.workflow:
                processable = processable.filter(workflow__contains=self.jobsource.workflow)
            if processable.count() > 0:
                self.exit_counter = 0
                logger.debug("Some BalsamJobs are still transitionable; will not quit")
                return
            if self.get_runnable().count() > 0:
                self.exit_counter = 0
                return
            else:
                self.exit_counter += 1
                logger.info(f"Nothing to do (exit counter {self.exit_counter}/10)")
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
            logger.info(f"MPIRun {run.job.cute_id} error code {retcode}:\n{tail}")
            run.free_workers()
        return run.current_state

    def timeout_kill(self, runs, timeout=10):
        for run in runs:
            run.process.terminate()  # SIGTERM
        start = time.time()
        for run in runs:
            try:
                run.process.wait(timeout=timeout)
            except:
                break
            if time.time() - start > timeout:
                break
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
            models.safe_select(BalsamJob.objects.filter(pk__in=error_pks))
            for run in by_states['RUN_ERROR']:
                run.job.refresh_from_db()
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
        '''queryset: jobs that can finish on idle workers (disregarding time limits)'''
        manager = self.jobsource
        num_idle = len(self.worker_group.idle_workers())
        if num_idle == 0:
            logger.debug(f'No idle worker nodes to run jobs')
            return manager.none()
        else:
            logger.debug(f'{num_idle} idle worker nodes')

        return manager.get_runnable(
            max_nodes=num_idle,
            order_by=('-num_nodes', '-wall_time_minutes')
        )

    def report_constrained(self):
        now = time.time()
        elapsed = now - self.last_report
        if elapsed < 10:
            return
        else:
            self.last_report = now
        num_idle = len(self.worker_group.idle_workers())
        logger.info(f'{num_idle} idle worker nodes')
        all_runnable = BalsamJob.objects.filter(state__in=models.RUNNABLE_STATES)
        unlocked = all_runnable.filter(lock='')
        logger.info('No runnable jobs')
        logger.info(f'{all_runnable.count()} runnable jobs across entire Balsam DB')
        logger.info(f'{unlocked.count()} of these are unlocked')
        if self.jobsource.workflow:
            unlocked = unlocked.filter(workflow__contains=self.jobsource.workflow)
            logger.info(f'{unlocked.count()} of these match the current workflow filter')
        too_large = unlocked.filter(num_nodes__gt=num_idle).count()
        if too_large > 0:
            logger.info(f'{too_large} of these could run now; but require more than {num_idle} nodes.')

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
            self.report_constrained()
            return

        # pre-assign jobs to nodes (descending order of node count)
        cache = list(runnable[:max_acquire])
        pre_assignments = []
        idx = 0
        while idx < len(cache):
            job = cache[idx]
            workers = self.worker_group.request(job.num_nodes)
            if workers:
                pre_assignments.append((job, workers))
                idx += 1
            else:
                num_idle = sum(w.num_nodes for w in self.worker_group.idle_workers())
                assert job.num_nodes > num_idle
                idx = next((i for i, job in enumerate(cache[idx:], idx) if
                            job.num_nodes <= num_idle), len(cache))

        # acquire lock on jobs
        to_acquire = [job.pk for (job, workers) in pre_assignments]
        acquired_pks = self.jobsource.acquire(to_acquire)
        logger.debug(f'Acquired lock on {len(acquired_pks)} out of {len(pre_assignments)} jobs marked for running')

        # dispatch runners; release workers that did not acquire job
        for (job, workers) in pre_assignments:
            if job.pk in acquired_pks:
                run = MPIRun(job, workers)
                self.mpi_runs.append(run)
            else:
                for w in workers:
                    w.idle = True
        BalsamJob.batch_update_state(acquired_pks, 'RUNNING', self.RUN_MESSAGE)

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
            logger.debug('EXIT: breaking launcher service loop')
            self.update(timeout=True)
            assert not self.is_active
            logger.info('Exit: All MPI runs terminated')
            self.jobsource.release_all_owned()
            logger.info('Exit: Launcher Released all BalsamJob locks')

    @property
    def is_active(self):
        return len(self.mpi_runs) > 0


class SerialLauncher:
    ZMQ_ENSEMBLE_EXE = find_spec("balsam.launcher.serial_mode_timed").origin

    def __init__(self, wf_name=None, time_limit_minutes=60, gpus_per_node=None,
                 persistent=False, limit_nodes=None, offset_nodes=None):
        self.wf_name = wf_name
        self.gpus_per_node = gpus_per_node
        self.is_persistent = persistent

        timer = remaining_time_minutes(time_limit_minutes)
        minutes_left = max(0.1, next(timer) - 1)
        self.worker_group = worker.WorkerGroup(limit=limit_nodes, offset=offset_nodes)
        num_workers = len(self.worker_group)

        hostnames = sorted([w.hostname for w in self.worker_group])
        master_host = hostnames[0]
        master_port = 19876
        timestamp = datetime.now().strftime('%Y-%m-%d_%H%M%S')
        log_fname = f'serial-ensemble_{timestamp}.log'

        self.total_nodes = sum(w.num_nodes for w in self.worker_group)
        os.environ['BALSAM_LAUNCHER_NODES'] = str(self.total_nodes)
        os.environ['BALSAM_JOB_MODE'] = "serial"

        self.app_cmd = f"{sys.executable} {self.ZMQ_ENSEMBLE_EXE}"
        self.app_cmd += f" --time-limit-min={minutes_left}"
        self.app_cmd += f" --master-address {master_host}:{master_port}"
        self.app_cmd += f" --log-filename {log_fname}"
        self.app_cmd += f" --num-workers {num_workers}"
        if self.wf_name:
            self.app_cmd += f" --wf-name={self.wf_name}"
        if self.gpus_per_node:
            self.app_cmd += f" --gpus-per-node={self.gpus_per_node}"
        if self.is_persistent:
            self.app_cmd += " --persistent"

    def run(self):
        global EXIT_FLAG
        workers = self.worker_group
        num_ranks = self.total_nodes
        rpn = 1
        mpi_str = workers.mpi_cmd(
            workers, app_cmd=self.app_cmd, num_ranks=num_ranks, ranks_per_node=rpn,
            cpu_affinity='none', envs={})
        logger.info(f'Starting MPI Fork ensemble process:\n{mpi_str}')

        self.outfile = open(os.path.join(settings.LOGGING_DIRECTORY, 'ensemble.out'), 'wb')
        self.process = subprocess.Popen(args=shlex.split(mpi_str), bufsize=1,
                                        stdout=self.outfile, stderr=subprocess.STDOUT,
                                        shell=False)

        while not EXIT_FLAG:
            try:
                retcode = self.process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                pass
            else:
                logger.info(f'ensemble pull subprocess returned {retcode}')
                break
        logger.debug("EXIT_FLAG has been flipped!")

        # do not directly kill a srun job step (would only kill srun; task continues to run)
        # TODO(KGF): see above comments about encapsulating within MPIRun derived class
        if settings.SCHEDULER_CLASS == "SlurmScheduler":
            # Use scancel insteak of pkill, kill, etc.
            sched_id = JobEnv.current_scheduler_id
            term_str = f"scancel --signal=TERM {sched_id}.0"
            logger.debug(f"Invoking Slurm scancel for mpi_ensemble job step: \"{term_str}\"")
            # TODO(KGF): calling scancel here may not be necessary, since "scancel --batch JOB_ID"
            # might already correctly call scancel on srun steps after the parent batch script
            # finishes handling the original/explicit SIGTERM here
            subprocess.Popen(args=shlex.split(term_str), bufsize=1,
                             stdout=self.outfile, stderr=subprocess.STDOUT,
                             shell=False)
            # TODO(KGF): consider sending SIGKILL via Slurm daemon if 1st scancel fails
        else:
            logger.debug(f"Sending SIGTERM to mpi_ensemble (pid={self.process.pid})")
            self.process.terminate()
            try:
                logger.debug("Waiting on mpi_ensemble to stop...")
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                logger.debug("No response in 10 seconds! Sending SIGKILL!")
                self.process.kill()
            logger.debug(f"Child mpi_ensemble process return code={self.process.returncode}")
        self.outfile.close()
        logger.info("ensemble.out file closed.")


def main(args):
    signal.signal(signal.SIGINT,  sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    wf_filter = args.wf_filter
    job_mode = args.job_mode
    timelimit_min = args.time_limit_minutes
    persistent = args.persistent
    nthread = (args.num_transition_threads if args.num_transition_threads
               else settings.NUM_TRANSITION_THREADS)
    gpus_per_node = args.gpus_per_node
    limit_nodes = args.limit_nodes
    offset_nodes = args.offset_nodes

    Launcher = MPILauncher if job_mode == 'mpi' else SerialLauncher

    try:
        if nthread > 0:
            transition_pool = transitions.TransitionProcessPool(nthread, wf_filter)
        else:
            transition_pool = None
        launcher = Launcher(wf_filter, timelimit_min, gpus_per_node, persistent,
                            limit_nodes, offset_nodes)
        launcher.run()
    except:
        raise
    finally:
        if transition_pool is not None:
            transition_pool.terminate()
        logger.info("Exit: Launcher exit graceful\n\n")


def get_args(inputcmd=None):
    '''Parse command line arguments'''
    parser = config_launcher_subparser()
    if inputcmd:
        return parser.parse_args(inputcmd)
    else:
        return parser.parse_args()


if __name__ == "__main__":
    setup()
    args = get_args()
    config_logging('launcher')
    logger.info("Loading Balsam Launcher")
    main(args)
