'''The Launcher is either invoked by the user, who bypasses the Balsam
scheduling service and submits directly to a local job queue, or by the
Balsam service metascheduler'''
import argparse
import os
import multiprocessing
import queue
import time

from django.conf import settings

import balsam.models
from balsam.models import BalsamJob
from balsam import scheduler

START_TIME = time.time() + 10.0

class BalsamLauncherException(Exception): pass

class Worker:
    def __init__(self, id, *, shape=None, block=None, corner=None,
                 ranks_per_worker=None):
        self.id = id
        self.shape = shape
        self.block = block
        self.corner = corner
        self.ranks_per_worker = ranks_per_worker
        self.idle = True

class WorkerGroup:

    def __init__(self, config):
        self.host_type = config.host_type
        self.partition = config.partition
        self.workers = []
        self.setup = getattr(self, f"setup_{self.host_type}")
        if self.host_type == 'DEFAULT':
            self.num_workers = config.num_workers
        else:
            self.num_workers = None
        self.setup()

    def setup_CRAY(self):
        node_ids = []
        ranges = self.partition.split(',')
        for node_range in ranges:
            lo, *hi = node_range.split('-')
            lo = int(lo)
            if hi:
                hi = int(hi[0])
                node_ids.extend(list(range(lo, hi+1)))
            else:
                node_ids.append(lo)
        for id in node_ids:
            self.workers.append(Worker(id))

    def setup_BGQ(self):
        # Boot blocks
        # Get (block, corner, shape) args for each sub-block
        pass

    def setup_DEFAULT(self):
        for i in range(self.num_workers):
            self.workers.apppend(Worker(i))

    def get_idle_workers(self):
        return [w for w in self.workers if w.idle]


SIGTIMEOUT = 'TIMEOUT!'
SIGNALS = {
    signal.SIGINT: 'SIG_INT',
    signal.SIGTERM: 'SIG_TERM',
}

class JobRetriever:
    '''Use the get_jobs method to pull valid jobs for this run'''

    def __init__(self, config):
        self.job_pk_list = None
        self._job_file = config.job_file
        self.wf_name = config.wf_name
        self.host_type = config.host_type

    def get_jobs(self):
        if self._job_file:
            jobs = self._jobs_from_file()
        else:
            jobs = self._jobs_from_wf(wf=self.wf_name)
        return self._filter(jobs)

    def _filter(self, jobs):
        jobs = jobs.exclude(state__in=balsam.models.END_STATES)
        jobs = jobs.filter(allowed_work_sites__icontains=settings.BALSAM_SITE)
        # Exclude jobs that are already in LauncherConfig pulled_jobs
        # Otherwise, you'll be calling job.idle() and qstating too much
        return [j for j in jobs if j.idle()]

    def _jobs_from_file(self):
        if self._job_pk_list is None:
            try:
                pk_strings = open(self._job_file).read().split()
            except IOError as e:
                raise BalsamLauncherException(f"Can't read {self._job_file}") from e
            try:
                self._job_pk_list = [uuid.UUID(pk) for pk in pk_strings]
            except ValueError:
                raise BalsamLauncherException(f"{self._job_file} contains bad UUID strings")
        try:
            jobs = BalsamJob.objects.filter(job_id__in=self._job_file_pk_list)
        except Exception as e:
            raise BalsamLauncherException("Failed to query BalsamJobDB") from e
        else: 
            return jobs
    
    def _jobs_from_wf(self, wf=''):
        objects = BalsamJob.objects
        try:
            jobs = objects.filter(workflow=wf) if wf else objects.all()
        except Exception as e:
            raise BalsamLauncherException(f"Failed to query BalsamJobDB for '{wf}'") from e
        else: 
            self._job_pk_list = [job.pk for job in jobs]
            return jobs
            
class LauncherConfig:
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

    def check_timeout(self, active_runners):
        if self.remaining_time_seconds() < 1.0:
            for runner in active_runners:
                runner.timeout(SIGTIMEOUT, None)
            return True
        return False

class TransitionProcessPool:
    TRANSITIONS = {
        'CREATED': check_parents,
        'LAUNCHER_QUEUED': check_parents,
        'AWAITING_PARENTS': check_parents,
        'READY': stage_in,
        'STAGED_IN': preprocess,
        'RUN_DONE': postprocess,
        'RUN_TIMEOUT': postprocess,
        'RUN_ERROR': postprocess,
        'POSTPROCESSED': stage_out
    }
    def __init__(self, num_transitions=None):
        if not num_transitions:
            num_transitions = settings.BALSAM_MAX_CONCURRENT_TRANSITIONS
        
        self.job_queue = multiprocessing.Queue()
        self.status_queue = multiprocessing.Queue()

        self.procs = [
            multiprocessing.Process( target=transitions.main, 
                                    args=(self.job_queue, self.status_queue))
            for i in range(num_transitions)
        ]
        for proc in self.procs:
            proc.start()

    def add_job(self, pk, transition_function):
        m = transitions.JobMsg(pk, transition_function)
        self.job_queue.put(m)

    def get_statuses():
        while not self.status_queue.empty():
            try:
                yield self.status_queue.get_nowait()
            except queue.Empty:
                break

    def stop_processes(self):
        while not self.job_queue.empty():
            try:
                self.job_queue.get_nowait()
            except queue.Empty:
                break

        m = transitions.JobMsg('end', None)
        for proc in self.procs:
            self.job_queue.put(m)

def main(args):
    launcher_config = LauncherConfig(args)
    job_retriever = JobRetriever(launcher_config)
    workers = WorkerGroup(launcher_config)

    transitions_pool = TransitionProcessPool()

    while not launcher_config.check_timeout():
        # keep a list of jobs I'm handling
        # get_jobs() should only fetch new ones
        # ping jobs I'm handling using job.service_ping
        jobs = job_retriever.get_jobs()
    
    transitions_pool.stop_processes()

    # Maintain up to 50 active runners (1 runner tracks 1 subprocess-aprun)
    # Add transitions to error_handle all the RUN_TIMEOUT jobs

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start Balsam Job Launcher.")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--job-file', help="File of Balsam job IDs")
    group.add_argument('--consume-all', action='store_true', 
                        help="Continuously run all jobs from DB")
    group.add_argument('--consume-wf', 
                       help="Continuously run jobs of specified workflow")

    parser.add_argument('--num-workers', type=int, default=1,
                        help="Theta: defaults to # nodes. BGQ: the # of subblocks")
    parser.add_argument('--serial-jobs-per-worker', type=int, default=4,
                        help="For non-MPI jobs, how many to pack per worker")
    parser.add_argument('--time-limit-minutes', type=int,
                        help="Override auto-detected walltime limit (runs
                        forever if no limit is detected or specified)")
    args = parser.parse_args()
    main(args)
