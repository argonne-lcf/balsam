'''A Runner is constructed with a list of jobs and a list of idle workers. It
creates and monitors the execution subprocess, updating job states in the DB as
necessary. RunnerGroup has a collection of Runner objects, logic for creating
the next Runner (i.e. assigning jobs to nodes), and the public interface to
monitor runners'''

# TODO: "balsam qsub" is misleading because you can't qsub a script that calls
# mpirun.  Implement a --mode script option that uses a "ScriptRunner". The
# ScriptRunner should parse the script to make sure it is not using more nodes
# than requested by job; and perhaps modify each mpirun with the correct
# workers_string

import functools
from math import ceil
import os
from pathlib import Path
import shlex
import sys
from subprocess import Popen, PIPE, STDOUT
from tempfile import NamedTemporaryFile
from threading import Thread
from queue import Queue, Empty

from django.conf import settings
from django.db import transaction

from balsam.models import InvalidStateError
from balsamlauncher import mpi_commands
from balsamlauncher.exceptions import *
from balsamlauncher.util import cd, get_tail

import logging
logger = logging.getLogger(__name__)
    
from importlib.util import find_spec
MPI_ENSEMBLE_EXE = find_spec("balsamlauncher.mpi_ensemble").origin


class MonitorStream(Thread):
    '''Thread: non-blocking read of a process's stdout'''
    def __init__(self, runner_output):
        super().__init__()
        self.stream = runner_output
        self.queue = Queue()
        self.daemon = True

    def run(self):
        # Call readline until empty string is returned
        for line in iter(self.stream.readline, b''):
            self.queue.put(line.decode('utf-8'))
        self.stream.close()

    def available_lines(self):
        while True:
            try: yield self.queue.get_nowait()
            except Empty: return


class Runner:
    '''Spawns ONE subprocess to run specified job(s) and monitor their execution'''
    def __init__(self, job_list, worker_list):
        host_type = worker_list[0].host_type
        assert all(w.host_type == host_type for w in worker_list)
        self.worker_list = worker_list
        mpi_cmd_class = getattr(mpi_commands, f"{host_type}MPICommand")
        self.mpi_cmd = mpi_cmd_class()
        self.jobs = job_list
        self.jobs_by_pk = {str(job.pk) : job for job in self.jobs}
        self.process = None
        self.monitor = None
        self.outfile = None
        self.popen_args = {}

    def start(self):
        self.process = Popen(**self.popen_args)
        if self.popen_args['stdout'] == PIPE:
            self.monitor = MonitorStream(self.process.stdout)
            self.monitor.start()

    def update_jobs(self):
        raise NotImplementedError

    def finished(self):
        return self.process.poll() is not None

    def timeout(self):
        self.process.terminate()
        for job in self.jobs:
            if job.state == 'RUNNING': 
                job.update_state('RUN_TIMEOUT')
                logger.info(f"Runner job {job.cute_id}: RUN_TIMEOUT")

class MPIRunner(Runner):
    '''One subprocess, one job'''
    def __init__(self, job_list, worker_list):

        super().__init__(job_list, worker_list)
        if len(self.jobs) != 1:
            raise BalsamRunnerError('MPIRunner must take exactly 1 job')

        job = self.jobs[0]
        envs = job.get_envs() # dict
        app_cmd = job.app_cmd
        nranks = job.num_ranks
        rpn = job.ranks_per_node
        tpr = job.threads_per_rank
        tpc = job.threads_per_core

        # Note that environment variables are passed through the MPI run command
        # line, rather than Popen directly, due to ALCF restrictions: 
        # https://www.alcf.anl.gov/user-guides/running-jobs-xc40#environment-variables
        mpi_str = self.mpi_cmd(worker_list, app_cmd=app_cmd, envs=envs,
                               num_ranks=nranks, ranks_per_node=rpn,
                               threads_per_rank=tpr, threads_per_core=tpc)
        
        basename = os.path.basename(job.working_directory)
        outname = os.path.join(job.working_directory, f"{basename}.out")
        self.outfile = open(outname, 'w+b')
        self.popen_args['args'] = shlex.split(mpi_str)
        self.popen_args['cwd'] = job.working_directory
        self.popen_args['stdout'] = self.outfile
        self.popen_args['stderr'] = STDOUT
        self.popen_args['bufsize'] = 1
        logger.info(f"MPIRunner {job.cute_id} Popen:\n{self.popen_args['args']}")
        logger.info(f"MPIRunner: writing output to {outname}")

    def update_jobs(self):
        job = self.jobs[0]
        #job.refresh_from_db() # TODO: handle RecordModified
        retcode = self.process.poll()
        if retcode == None:
            logger.debug(f"MPIRunner {job.cute_id} still running")
            curstate = 'RUNNING'
            msg = ''
        elif retcode == 0:
            logger.info(f"MPIRunner {job.cute_id} return code 0: done")
            curstate = 'RUN_DONE'
            msg = ''
        else:
            curstate = 'RUN_ERROR'
            self.process.communicate()
            self.outfile.close()
            tail = get_tail(self.outfile.name)
            msg = f"MPIRunner {job.cute_id} RETURN CODE {retcode}:\n{tail}"
            logger.info(msg)
        if job.state != curstate: job.update_state(curstate, msg) # TODO: handle RecordModified



class MPIEnsembleRunner(Runner):
    '''One subprocess: an ensemble of serial jobs run in an mpi4py wrapper'''
    def __init__(self, job_list, worker_list):

        super().__init__(job_list, worker_list)
        root_dir = Path(self.jobs[0].working_directory).parent
        
        self.popen_args['bufsize'] = 1
        self.popen_args['stdout'] = PIPE
        self.popen_args['stderr'] = STDOUT
        self.popen_args['cwd'] = root_dir

        # mpi_ensemble.py reads jobs from this temp file
        with NamedTemporaryFile(prefix='mpi-ensemble', dir=root_dir, 
                                delete=False, mode='w') as fp:
            ensemble_filename = os.path.abspath(fp.name)
            for job in self.jobs:
                cmd = job.app_cmd
                fp.write(f"{job.pk} {job.working_directory} {cmd}\n")

        logger.info('MPIEnsemble handling jobs: '
                    f' {", ".join(j.cute_id for j in self.jobs)} '
                   )
        rpn = worker_list[0].max_ranks_per_node
        nranks = sum(w.num_nodes*rpn for w in worker_list)
        envs = self.jobs[0].get_envs() # TODO: is pulling envs in runner inefficient?
        app_cmd = f"{sys.executable} {MPI_ENSEMBLE_EXE} {ensemble_filename}"

        mpi_str = self.mpi_cmd(worker_list, app_cmd=app_cmd, envs=envs,
                               num_ranks=nranks, ranks_per_node=rpn)

        self.popen_args['args'] = shlex.split(mpi_str)
        logger.info(f"MPIEnsemble Popen:\n {self.popen_args['args']}")

    def update_jobs(self):
        '''Relies on stdout of mpi_ensemble.py'''
        retcode = self.process.poll()
        if retcode not in [None, 0]:
            msg = "mpi_ensemble.py had nonzero return code:\n"
            msg += "".join(self.monitor.available_lines())
            logger.exception(msg)

        logger.debug("Checking mpi_ensemble stdout for status updates...")
        for line in self.monitor.available_lines():
            try:
                pk, state, *msg = line.split()
                msg = ' '.join(msg)
                job = self.jobs_by_pk[pk]
                job.update_state(state, msg) # TODO: handle RecordModified exception
                logger.info(f"MPIEnsemble {job.cute_id} updated to {state}: {msg}")
            except (ValueError, KeyError, InvalidStateError) as e:
                logger.error(f"Invalid statusMsg from mpi_ensemble: {line.strip()}")

class RunnerGroup:
    
    MAX_CONCURRENT_RUNNERS = settings.BALSAM_MAX_CONCURRENT_RUNNERS
    def __init__(self, lock):
        self.runners = []
        self.lock = lock

    def __iter__(self):
        return iter(self.runners)
    
    def create_next_runner(self, runnable_jobs, workers):
        '''Implements one particular strategy for choosing the next job, assuming
        all jobs are either single-process or MPI-parallel. Will return the serial
        ensemble job or single MPI job that occupies the largest possible number of
        idle nodes'''

        if len(self.runners) == self.MAX_CONCURRENT_RUNNERS:
            raise ExceededMaxRunners(
                f"Cannot have more than {self.MAX_CONCURRENT_RUNNERS} simultaneous runners"
            )

        idle_workers = [w for w in workers if w.idle]
        nidle_workers = len(idle_workers)
        if nidle_workers == 0:
            raise NoAvailableWorkers

        nodes_per_worker = workers[0].num_nodes
        rpn = workers[0].max_ranks_per_node
        assert all(w.num_nodes == nodes_per_worker for w in idle_workers)
        assert all(w.max_ranks_per_node == rpn for w in idle_workers)
        logger.debug(f"Available workers: {nidle_workers} idle with "
            f"{nodes_per_worker} nodes per worker")
        nidle_nodes =  nidle_workers * nodes_per_worker
        nidle_ranks = nidle_nodes * rpn

        serial_jobs = [j for j in runnable_jobs if j.num_ranks == 1]
        nserial = len(serial_jobs)
        logger.debug(f"{nserial} single-process jobs can run")

        mpi_jobs = [j for j in runnable_jobs if 1 < j.num_nodes <= nidle_nodes or
                    (1==j.num_nodes<=nidle_nodes and j.ranks_per_node > 1)]
        largest_mpi_job = (max(mpi_jobs, key=lambda job: job.num_nodes) 
                           if mpi_jobs else None)
        if largest_mpi_job:
            logger.debug(f"{len(mpi_jobs)} MPI jobs can run; largest requires "
            f"{largest_mpi_job.num_nodes} nodes")
        else:
            logger.debug("No MPI jobs can run")
        
        # Try to fill all available nodes with serial ensemble runner
        # If there are not enough serial jobs; run the larger of:
        # largest MPI job that fits, or the remaining serial jobs
        if nserial >= nidle_ranks:
            jobs = serial_jobs[:nidle_ranks] # TODO: try putting ALL serial jobs into one MPIEnsemble
            assigned_workers = idle_workers
            runner_class = MPIEnsembleRunner
            msg = (f"Running {len(jobs)} serial jobs on {nidle_workers} workers "
            f"with {nodes_per_worker} nodes-per-worker and {rpn} ranks per node")
        elif largest_mpi_job and largest_mpi_job.num_nodes > nserial // rpn:
            jobs = [largest_mpi_job]
            num_workers = ceil(largest_mpi_job.num_nodes / nodes_per_worker)
            assigned_workers = idle_workers[:num_workers]
            runner_class = MPIRunner
            msg = (f"Running {largest_mpi_job.num_nodes}-node MPI job")
        else:
            jobs = serial_jobs
            nworkers = ceil(nserial/rpn/nodes_per_worker)
            assigned_workers = idle_workers[:nworkers]
            runner_class = MPIEnsembleRunner
            msg = (f"Running {len(jobs)} serial jobs on {nworkers} workers "
                        f"totalling {nworkers*nodes_per_worker} nodes "
                        f"with {rpn} ranks per worker")
        
        if not jobs: 
            raise NoAvailableWorkers
        else:
            logger.info(msg)

        runner = runner_class(jobs, assigned_workers)
        runner.start()
        self.runners.append(runner)
        for worker in assigned_workers: worker.idle = False

    def update_and_remove_finished(self):
        # TODO: Benchmark performance overhead; does grouping into one
        # transaction save significantly?
        logger.debug(f"Checking status of {len(self.runners)} active runners")
        any_finished = False
        self.lock.acquire()
        for runner in self.runners: runner.update_jobs()
        self.lock.release()

        finished_runners = (r for r in self.runners if r.finished())

        for runner in finished_runners:
            if any(j.state not in ['RUN_DONE','RUN_ERROR','RUN_TIMEOUT'] for j in runner.jobs):
                self.lock.acquire()
                runner.update_jobs()
                self.lock.release()
            if any(j.state not in ['RUN_DONE','RUN_ERROR','RUN_TIMEOUT'] for j in runner.jobs):
                msg = (f"Job {job.cute_id} runner process done, but failed to update job state.")
                logger.exception(msg)
                raise RuntimeError(msg)
            else:
                any_finished = True
                self.runners.remove(runner)
                for worker in runner.worker_list:
                    worker.idle = True
        return any_finished

    @property
    def running_job_pks(self):
        return [j.pk for runner in self.runners for j in runner.jobs]
