from collections import namedtuple
import functools
import signal
import sys
from subprocess import Popen, PIPE
from threading import Thread
from queue import Queue, Empty

import balsam.models
from launcher import SIGNALS, BGQ_HOSTS, CRAY_HOSTS

class BalsamRunnerException(Exception): pass

Status = namedtuple('Status', ['id', 'state', 'msg'])

class cd:
    '''Context manager for changing cwd'''
    def __init__(self, new_path):
        self.new_path = os.path.expanduser(new_path)

    def __enter__(self):
        self.saved_path = os.getcwd()
        os.chdir(self.new_path)

    def __exit__(self):
        os.chdir(self.saved_path)


class RunnerConfig(object):
    def __init__(self, host_type):
        if host_type is None:
            self.mpi = 'mpirun'
            self.nproc = '-n'
            self.ppn = '-ppn'
            self.env = '-env'
            self.cpu_binding = None
            self.threads_per_rank = None
            self.threads_per_core = None
        elif host_type == 'BGQ':
            self.mpi = 'runjob'
            self.nproc = '--np'
            self.ppn = '-p'
            self.env = '--envs' # VAR1=val1:VAR2=val2
            self.cpu_binding = None
            self.threads_per_rank = None
            self.threads_per_core = None
        elif host_type == 'CRAY':
            # 64 independent jobs, 1 per core of a KNL node: -n64 -N64 -d1 -j1
            self.mpi = 'aprun'
            self.nproc = '-n'
            self.ppn = '-N'
            self.env = '--env' # VAR1=val1:VAR2=val2
            self.cpu_binding = '-cc depth'
            self.threads_per_rank = '-d'
            self.threads_per_core = '-j'

    def command(self, workers):
        self.set_workers_string(workers)
        return f"{self.mpi}"


class Monitor(Thread):
    '''Thread for non-blocking read of Runner's subprocess stdout'''
    def __init__(self, runner_output):
        self.stream = runner_output
        self.queue = Queue()
        self.daemon = True

    def run(self):
        # Call readline until empty string is returned
        for line in iter(self.stream.readline, b''):
            id, state, *msg = line.split()
            msg = ' '.join(msg)
            self.queue.put(Status(id, state, msg))
        self.stream.close()


class Runner:
    '''Spawns ONE subprocess to run specified job(s) and monitor their execution'''
    def __init__(self, job_list, host_type):
        self.jobs = job_list
        self.jobs_by_pk = {job.pk : job for job in self.jobs}
        self.host_type = host_type
        self.process = None
        self.monitor = None
        self.popen_args = {'bufsize':1, 'stdout':PIPE}
        self.cmd_generator = None

    def start(self):
        for signum in SIGNALS:
            signal.signal(sigum, self.timeout)
        self.process = Popen(**self.popen_args)
        self.monitor = Monitor(self.process.stdout)
        self.monitor.start()

    def update_jobs(self):
        pass

    def timeout(self, signum, stack):
        sig_msg = SIGNALS.get(signum, signum)
        message = f"{self.__class__.__name__} got signal {sig_msg}"
        self.update_jobs()
        self.process.terminate()
        for job in self.jobs:
            if job.state == 'RUNNING':
                job.update_state('RUN_TIMEOUT', message)

class MPIRunner(Runner):
    '''One subprocess: one mpi invocation'''
    def __init__(self, cmd_generator, job):
        self.cmd_generator = cmd_generator 
        self.job = job
        

class MPIEnsembleRunner(Runner):
    '''One subprocess: an ensemble of serial jobs run in an mpi4py wrapper'''
    def __init__(self, job_list, worker_list)
    def update_jobs(self):
        while True:
            try:
                status = self.monitor.queue.get_nowait()
            except Empty:
                return
            else:
                pk, state, msg = status
                if pk in self.jobs_by_pk and state in balsam.models.STATES:
                    self.jobs_by_pk[id].update_state(state, msg) # TODO: handle RecordModified exception
                else:
                    raise BalsamRunnerException(f"Invalid status update: {status}")
