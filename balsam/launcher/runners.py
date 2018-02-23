'''
The actual execution of BalsamJob applications occurs in **Runners** which
delegate a list of **BalsamJobs** to one or more **Workers**, which are an
abstraction for the local computing resources. Each **Runner** subclass hides
system-dependent MPI details and executes jobs in a particular mode, such as:

    * ``MPIRunner``: a single multi-node job
    * ``MPIEnsembleRunner``: several serial jobs packaged into one ensemble

.. note:: 
    The current command line tool ``balsam qsub`` is misleading because users
    can't qsub a script that calls mpirun.  We must implement a ``--mode
    script`` option that results in jobs which are handled by a
    ``ScriptRunner``. The ScriptRunner should parse the script to make sure it
    is not using more nodes than requested by the job; and perhaps identify and
    translate each ``mpirun`` in the script to the correct, local
    system-specific command.
'''

import functools
from math import ceil
import os
from pathlib import Path
import shlex
import sys
from subprocess import Popen, PIPE, STDOUT
from tempfile import NamedTemporaryFile
from queue import Queue, Empty

from django.conf import settings
from django.db import transaction

from balsam.service.models import InvalidStateError
from balsam.launcher import mpi_commands
from balsam.launcher.exceptions import *
from balsam.launcher.util import cd, get_tail, parse_real_time

import logging
logger = logging.getLogger(__name__)
    
from importlib.util import find_spec
MPI_ENSEMBLE_EXE = find_spec("balsam.launcher.mpi_ensemble").origin


class Runner:
    '''Runner Base Class: constructor and methods for starting application
    subprocesses.
    
    Each Runner instance manages one Python ``subprocess`` and is destroyed when
    the subprocess is no longer tracked. The Runner may manage and monitor the
    execution of one or many BalsamJobs.
    '''

    def __init__(self, job_list, worker_list):
        '''Instantiate a new Runner instance.

        This method should be extended by concrete Runner subclasses.
        A Runner is constructed with a list of BalsamJobs and a list of idle workers. 
        After the ``__init__`` method is finished, the instance ``popen_args``
        attribute should contain all the arguments to ``subprocess.Popen`` that
        are necessary to start the Runner.

        Args:
            - ``job_list`` (*list*): A list of *BalsamJobs* to be executed
            - ``worker_list``: A list of *Workers* that will run the BalsamJob
              applications
        '''
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
        '''Start the Runner subprocess.

        If the Popen stdout argument is PIPE, then a separate MonitorStream
        thread is started, which is used to check the output of the Runner
        subprocess in a non-blocking fashion.
        '''
        self.process = Popen(**self.popen_args)
        if self.popen_args['stdout'] == PIPE:
            self.monitor = MonitorStream(self.process.stdout)
            self.monitor.start()

    def update_jobs(self, timeout=False):
        '''Monitor the execution subprocess and update job states in the DB 
        
        Args:
            -``timeout`` (*bool*): If *True*, then jobs that are currently in
            state RUNNING under this Runner will be timed-out and marked in
            state RUN_TIMEOUT. Defaults to *False*.

        Returns:
            -``finished`` (*bool*): If *True*, then the managing ``RunnerGroup``
            will assume this Runner is finished and remove it
        '''
        raise NotImplementedError

    def finished(self):
        '''Return *True* if the Runner subprocess has finished'''
        return self.process.poll() is not None

class MPIRunner(Runner):
    '''Manage one mpirun subprocess for one multi-node job'''
    def __init__(self, job_list, worker_list):
        '''Create the command line for mpirun'''
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
        
        mpi_str = f'time -p ( {mpi_str} )'
        basename = job.name
        outname = os.path.join(job.working_directory, f"{basename}.out")
        self.outfile = open(outname, 'w+b')
        self.popen_args['args'] = mpi_str
        self.popen_args['cwd'] = job.working_directory
        self.popen_args['stdout'] = self.outfile
        self.popen_args['stderr'] = STDOUT
        self.popen_args['shell'] = True
        self.popen_args['executable'] = '/bin/bash'
        self.popen_args['bufsize'] = 1
        logger.info(f"MPIRunner {job.cute_id} Popen:\n{self.popen_args['args']}")
        logger.info(f"MPIRunner: writing output to {outname}")

    def update_jobs(self, timeout=False):
        '''Update the job state and return finished flag'''
        job = self.jobs[0]

        retcode = self.process.poll()
        if retcode == None:
            logger.debug(f"MPIRunner {job.cute_id} still running")
            curstate = 'RUNNING'
            msg = ''
        elif retcode == 0:
            logger.info(f"MPIRunner {job.cute_id} return code 0: done")
            curstate = 'RUN_DONE'
            msg = ''
            self.outfile.close()
        else:
            curstate = 'RUN_ERROR'
            self.process.communicate()
            self.outfile.close()
            tail = get_tail(self.outfile.name)
            msg = f"MPIRunner {job.cute_id} RETURN CODE {retcode}:\n{tail}"
            logger.info(msg)

        if curstate in ['RUNNING', 'RUN_ERROR'] and timeout:
            curstate = 'RUN_TIMEOUT'
            msg = f"MPIRunner {job.cute_id} RUN_TIMEOUT"
            logger.info(msg)

        if job.state != curstate:
            if curstate == 'RUN_DONE':
                elapsed = parse_real_time(get_tail(self.outfile.name, indent=''))
                if elapsed:
                    job.runtime_seconds = float(elapsed)
                    job.save(update_fields=['runtime_seconds'])
            job.update_state(curstate, msg)
        else:
            job.refresh_from_db()

        finished = timeout or (retcode is not None)
        return finished


class MPIEnsembleRunner(Runner):
    '''Manage an ensemble of serial (non-MPI) jobs in one MPI subprocess
    
    Invokes the mpi_ensemble.py script, where the jobs are run in parallel
    across workers
    '''
   
    def __init__(self, job_list, worker_list):
        '''Create an mpi_ensemble file with jobs passed to the master-worker
        script'''
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
        os.chmod(ensemble_filename, 0o644)

        rpn = worker_list[0].max_ranks_per_node
        nranks = sum(w.num_nodes*rpn for w in worker_list)
        envs = self.jobs[0].get_envs() # TODO: is pulling envs in runner inefficient?
        app_cmd = f"{sys.executable} {MPI_ENSEMBLE_EXE} {ensemble_filename}"

        mpi_str = self.mpi_cmd(worker_list, app_cmd=app_cmd, envs=envs,
                               num_ranks=nranks, ranks_per_node=rpn)

        self.popen_args['args'] = shlex.split(mpi_str)
        logger.info(f"MPIEnsemble Popen:\n {self.popen_args['args']}")
        self.ensemble_filename = ensemble_filename

    def update_jobs(self, timeout=False):
        '''Update serial job states, according to the stdout of mpi_ensemble.py'''
        
        logger.debug("Checking mpi_ensemble stdout for status updates...")
        for line in self.monitor.available_lines():
            try:
                pk, state, *msg = line.split()
                msg = ' '.join(msg)
                job = self.jobs_by_pk[pk]
                job.update_state(state, msg) # TODO: handle RecordModified exception
                logger.info(f"MPIEnsemble {job.cute_id} updated to {state}: {msg}")
            except (ValueError, KeyError, InvalidStateError) as e:
                if 'resources: utime' not in line:
                    logger.error(f"Unexpected statusMsg from mpi_ensemble: {line.strip()}")
            else:
                if "elapsed seconds" in msg:
                    job.runtime_seconds = float(msg.split()[-1])
                    job.save(update_fields=['runtime_seconds'])

        retcode = None
        if timeout:
            for job in self.jobs:
                if job.state == 'RUNNING':
                    job.update_state('RUN_TIMEOUT', 'timed out during MPIEnsemble')
                    logger.debug(f"MPIEnsemble job {job.cute_id} RUN_TIMEOUT")
        else:
            retcode = self.process.poll()
            if retcode not in [None, 0]:
                msg = f"mpi_ensemble.py had nonzero return code: {retcode}\n"
                msg += "".join(self.monitor.available_lines())
                logger.exception(msg)
                for job in self.jobs:
                    if job.state != 'RUN_DONE':
                        job.update_state('FAILED', 'MPIEnsemble error')
        finished = timeout or (retcode is not None)
        return finished

class RunnerGroup:
    '''Iterable collection of Runner objects with logic for creating
    the next Runner (i.e. assigning jobs to nodes), and the public interface to
    monitor runners'''
    
    MAX_CONCURRENT_RUNNERS = settings.BALSAM_MAX_CONCURRENT_RUNNERS
    def __init__(self, lock):
        self.runners = []
        self.lock = lock

    def __iter__(self):
        return iter(self.runners)
    
    def create_next_runner(self, runnable_jobs, workers):
        '''Create the next Runner object, add it to the RunnerGroup collection,
        and start the Runner subprocess.
        
        This method implements one particular strategy for choosing the next
        job, assuming all jobs are either single-process or MPI-parallel. Will
        return the serial ensemble job or single MPI job that occupies the
        largest possible number of idle nodes.

        Args:
            - ``runnable_jobs``: list of ``BalsamJob`` objects that are ready to run
            - ``workers``: iterable container of all ``Worker`` instances; idle
              workers may be assigned to a Runner

        Raises:
            - ExceededMaxRunners: if the number of current Runners (and thereby
              background mpirun processes) goes over the user-configured threshold
            - NoAvailableWorkers: if no workers are idle
        '''

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
            jobs = serial_jobs[:2*nidle_ranks] # TODO:Expt w/ > 2 jobs per worker
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
        logger.debug(f"Using workers: {[w.id for w in assigned_workers]}")

    def update_and_remove_finished(self, timeout=False):
        '''Iterate over all Runners in this RunnerGroup, update the job
        statuses, and remove finished Runners from the collection. 
        
        Optionally timeout/kill any outstanding jobs (when it's time to quit).
        
        Args:
            - ``timeout`` (bool): If *True*, timeout and SIGTERM-pause-SIGKILL any
              outstanding subprocesses. Defaults to *False*

        Raises:
            - ``RuntimeError``: if a Runner reports that it is finished but
              failed to mark any of its BalsamJobs in a completed state
        '''
        # TODO: Benchmark performance overhead; does grouping into one
        # transaction save significantly?
        logger.debug(f"Checking status of {len(self.runners)} active runners")

        finished_runners = []
        
        self.lock.acquire()
        for i, runner in enumerate(self.runners): 
            logger.debug(f"updating runner {i}")
            finished = runner.update_jobs(timeout)
            if finished: finished_runners.append(runner)

        killed_runners = (r for r in self.runners if r not in finished_runners
                          and all(j.state=='USER_KILLED' for j in r.jobs))

        for runner in killed_runners:
            runner.process.terminate()
            try: runner.process.wait(timeout=10)
            except: runner.process.kill()
            finished_runners.append(runner)
        self.lock.release()

        if timeout:
            for runner in self.runners:
                runner.process.terminate()
                try: runner.process.wait(timeout=10)
                except: runner.process.kill()

        for runner in finished_runners:
            if any(j.state == 'RUNNING' for j in runner.jobs):
                msg = (f"Runner process done, but failed to update job state.")
                logger.exception(msg)
                raise RuntimeError(msg)
            else:
                self.runners.remove(runner)
                logger.debug(f"Freeing workers: {[w.id for w in runner.worker_list]}")
                for worker in runner.worker_list:
                    worker.idle = True

        any_finished = finished_runners != []
        return any_finished

    @property
    def running_job_pks(self):
        '''``@property``: return a flat list of all BalsamJob primary keys that are
        currently being handled by Runners in this RunnerGroup'''
        return [j.pk for runner in self.runners for j in runner.jobs]
