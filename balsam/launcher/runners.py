import functools
import os
from pathlib import Path
import signal
import shlex
import sys
from subprocess import Popen, PIPE, STDOUT
from tempfile import NamedTemporaryFile
from threading import Thread
from queue import Queue, Empty

import balsam.models
from balsam.launcher.launcher import SIGNALS
from balsam.launcher import mpi_commands
from balsam.launcher import mpi_ensemble

class BalsamRunnerException(Exception): pass

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
    def __init__(self, job_list, worker_list, host_type):
        mpi_cmd_class = getattr(mpi_commands, f"{host_type}MPICommand")
        self.mpi_cmd = mpi_cmd_class()
        self.jobs = job_list
        self.jobs_by_pk = {job.pk : job for job in self.jobs}
        self.process = None
        self.monitor = None
        self.outfile = None
        self.popen_args = {}

    def start(self):
        for signum in SIGNALS:
            signal.signal(sigum, self.timeout)
        self.process = Popen(**self.popen_args)
        if self.popen_args['stdout'] == PIPE:
            self.monitor = MonitorStream(self.process.stdout)
            self.monitor.start()

    def update_jobs(self):
        raise NotImplementedError

    @staticmethod
    def get_app_cmd(job):
        if job.application:
            app = ApplicationDefinition.objects.get(name=job.application)
            return f"{app.executable} {app.application_args}"
        else:
            return job.direct_command

    def timeout(self, signum, stack)
        sig_msg = SIGNALS.get(signum, signum)
        message = f"{self.__class__.__name__} got signal {sig_msg}"
        self.update_jobs()
        self.process.terminate()
        for job in self.jobs:
            if job.state == 'RUNNING':
                job.update_state('RUN_TIMEOUT', message)

class MPIRunner(Runner):
    '''One subprocess, one job'''
    def __init__(self, job_list, worker_list, host_type):

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
        if job.state != curstate:
            job.update_state(curstate, msg) # TODO: handle RecordModified


class MPIEnsembleRunner(Runner):
    '''One subprocess: an ensemble of serial jobs run in an mpi4py wrapper'''
    def __init__(self, job_list, worker_list, host_type):

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
            for job in self.job_list:
                cmd = self.get_app_cmd(job)
                fp.write(f"{job.pk} {job.working_directory} {cmd}\n")

        nproc = sum(w.ranks_per_worker for w in worker_list)
        mpi_str = self.mpi_cmd(self.jobs[0], worker_list, nproc=nproc)

        command = f"{mpi_str} {mpi_ensemble_exe} {self.ensemble_filename}"
        self.popen_args['args'] = shlex.split(command)

    def update_jobs(self):
        for line in self.monitor.available_lines():
            pk, state, *msg = line.split()
            msg = ' '.join(msg)
            if pk in self.jobs_by_pk and state in balsam.models.STATES:
                self.jobs_by_pk[id].update_state(state, msg) # TODO: handle RecordModified exception
            else:
                raise BalsamRunnerException(f"Invalid status update: {status}")
