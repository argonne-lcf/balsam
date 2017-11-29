'''A Runner is constructed with a list of jobs and a list of idle workers. It
creates and monitors the execution subprocess, updating job states in the DB as
necessary. RunnerGroup contains the list of Runner objects, logic for creating
the next Runner (i.e. assigning jobs to nodes), and the public interface'''

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

import balsam.models
from balsam.launcher import mpi_commands
from balsam.launcher import mpi_ensemble
from balsam.launcher.exceptions import *

class cd:
    '''Context manager for changing cwd'''
    def __init__(self, new_path):
        self.new_path = os.path.expanduser(new_path)

    def __enter__(self):
        self.saved_path = os.getcwd()
        os.chdir(self.new_path)

    def __exit__(self):
        os.chdir(self.saved_path)


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
        self.jobs_by_pk = {job.pk : job for job in self.jobs}
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

    @staticmethod
    def get_app_cmd(job):
        if job.application:
            app = ApplicationDefinition.objects.get(name=job.application)
            return f"{app.executable} {app.application_args}"
        else:
            return job.direct_command

    def timeout(self):
        self.process.terminate()
        with transaction.atomic():
            for job in self.jobs:
                if job.state == 'RUNNING': job.update_state('RUN_TIMEOUT')

class MPIRunner(Runner):
    '''One subprocess, one job'''
    def __init__(self, job_list, worker_list):

        super().__init__(job_list, worker_list)
        if len(self.jobs) != 1:
            raise BalsamRunnerException('MPIRunner must take exactly 1 job')

        job = self.jobs[0]
        app_cmd = self.get_app_cmd(job)

        mpi_str = self.mpi_cmd(job, worker_list)
        
        basename = os.path.basename(job.working_directory)
        outname = os.path.join(job.working_directory, f"{basename}.out")
        self.outfile = open(outname, 'w+b')
        command = f"{mpi_str} {app_cmd}"
        self.popen_args['args'] = shlex.split(command)
        self.popen_args['cwd'] = job.working_directory
        self.popen_args['stdout'] = self.outfile
        self.popen_args['stderr'] = STDOUT
        self.popen_args['bufsize'] = 1

    def update_jobs(self):
        job = self.jobs[0]
        #job.refresh_from_db() # TODO: handle RecordModified
        retcode = self.process.poll()
        if retcode == None:
            curstate = 'RUNNING'
            msg = ''
        elif retcode == 0:
            curstate = 'RUN_FINISHED'
            msg = ''
        else:
            curstate = 'RUN_ERROR'
            msg = str(retcode)
        if job.state != curstate: job.update_state(curstate, msg) # TODO: handle RecordModified


class MPIEnsembleRunner(Runner):
    '''One subprocess: an ensemble of serial jobs run in an mpi4py wrapper'''
    def __init__(self, job_list, worker_list):

        mpi_ensemble_exe = os.path.abspath(mpi_ensemble.__file__)

        super().__init__(job_list, worker_list)
        root_dir = Path(self.jobs[0].working_directory).parent
        
        self.popen_args['bufsize'] = 1
        self.popen_args['stdout'] = PIPE
        self.popen_args['stderr'] = STDOUT
        self.popen_args['cwd'] = root_dir

        with NamedTemporaryFile(prefix='mpi-ensemble', dir=root_dir, 
                                delete=False, mode='w') as fp:
            self.ensemble_filename = fp.name
            for job in self.jobs:
                cmd = self.get_app_cmd(job)
                fp.write(f"{job.pk} {job.working_directory} {cmd}\n")

        nproc = sum(w.ranks_per_worker for w in worker_list)
        mpi_str = self.mpi_cmd(self.jobs[0], worker_list, nproc=nproc)

        command = f"{mpi_str} {mpi_ensemble_exe} {self.ensemble_filename}"
        self.popen_args['args'] = shlex.split(command)

    def update_jobs(self):
        '''Relies on stdout of mpi_ensemble.py'''
        for line in self.monitor.available_lines():
            pk, state, *msg = line.split()
            msg = ' '.join(msg)
            if pk in self.jobs_by_pk and state in balsam.models.STATES:
                job = self.jobs_by_pk[pk]
                job.update_state(state, msg) # TODO: handle RecordModified exception
            else:
                raise BalsamRunnerException(f"Invalid status update: {status}")

class RunnerGroup:
    
    MAX_CONCURRENT_RUNNERS = settings.BALSAM_MAX_CONCURRENT_RUNNERS
    def __init__(self):
        self.runners = []

    def __iter__(self):
        return iter(self.runners)
    
    def create_next_runner(runnable_jobs, workers):
        '''Implements one particular strategy for choosing the next job, assuming
        all jobs are either single-process or MPI-parallel. Will return the serial
        ensemble job or single MPI job that occupies the largest possible number of
        idle nodes'''

        if len(self.runners) == MAX_CONCURRENT_RUNNERS:
            raise ExceededMaxRunners(
                f"Cannot have more than {MAX_CONCURRENT_RUNNERS} simultaneous runners"
            )

        idle_workers = [w for w in workers if w.idle]
        nidle = len(idle_workers)
        rpw = workers[0].ranks_per_worker
        assert all(w.ranks_per_worker == rpw for w in idle_workers)

        serial_jobs = [j for j in runnable_jobs if j.num_nodes == 1 and
                       j.processes_per_node == 1]
        nserial = len(serial_jobs)

        mpi_jobs = [j for j in runnable_jobs if 1 < j.num_nodes <= nidle or
                    (1==j.num_nodes<=nidle  and j.processes_per_node > 1)]
        largest_mpi_job = (max(mpi_jobs, key=lambda job: job.num_nodes) 
                           if mpi_jobs else None)
        
        if nserial >= nidle*rpw:
            jobs = serial_jobs[:nidle*rpw]
            assigned_workers = idle_workers
            runner_class = MPIEnsembleRunner
        elif largest_mpi_job and largest_mpi_job.num_nodes > nserial // rpw:
            jobs = [largest_mpi_job]
            assigned_workers = idle_workers[:largest_mpi_job.num_nodes]
            runner_class = MPIRunner
        else:
            jobs = serial_jobs
            assigned_workers = idle_workers[:ceil(float(nserial)/rpw)]
            runner_class = MPIEnsembleRunner
        
        if not jobs: raise NoAvailableWorkers

        runner = runner_class(jobs, assigned_workers)
        runner.start()
        self.runners.append(runner)
        for worker in assigned_workers: worker.idle = False

    def update_and_remove_finished(self):
        # TODO: Benchmark performance overhead; does grouping into one
        # transaction save significantly?
        any_finished = False
        with transaction.atomic():
            for runner in self.runners: runner.update_jobs()

        for runner in self.runners[:]:
            if runner.finished():
                any_finished = True
                self.runners.remove(runner)
                for worker in runner.worker_list:
                    worker.idle = True
        return any_finished

    @property
    def running_job_pks(self):
        active_runners = [r for r in self.runners if not r.finished()]
        return [j.pk for runner in active_runners for j in runner.jobs]
