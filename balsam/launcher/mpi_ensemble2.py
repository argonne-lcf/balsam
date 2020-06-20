'''mpi4py wrapper that allows an ensemble of serial applications to run in
parallel across ranks on the computing resource'''
import argparse
from collections import defaultdict
import os
import sys
import logging
import random
from subprocess import Popen, STDOUT, TimeoutExpired
import multiprocessing
import queue
import shlex
import signal
import time
import uuid
import psutil

_p = psutil.Process()
try:
    _p.cpu_affinity([])
    print("Detected psutil CPU affinity support.")
except AttributeError:
    class MockPsutilProcess:
        def cpu_affinity(self, list): pass
    _p = MockPsutilProcess()
    print("No psutil CPU Affinity support: will not bind processes to cores.")
    


from mpi4py import MPI
from django.db import transaction, connections

from balsam import config_logging, setup
setup()
from balsam.launcher.util import get_tail, remaining_time_minutes
from balsam.core.models import BalsamJob, safe_select, PROCESSABLE_STATES
from django.conf import settings

Queue = multiprocessing.Queue
try:
    Queue().qsize()
except NotImplementedError:
    from balsam.launcher.multi_queue_fallback import MyQueue
    Queue = MyQueue
    print("No queue.qsize support: will use fallback MyQueue implementation")


SERIAL_CORES_PER_NODE = settings.SERIAL_CORES_PER_NODE
SERIAL_HYPERTHREAD_STRIDE = settings.SERIAL_HYPERTHREAD_STRIDE
logger = logging.getLogger('balsam.launcher.mpi_ensemble')

comm = MPI.COMM_WORLD
RANK = comm.Get_rank()
MSG_BUFSIZE = 2**16 # 65 kB
connections.close_all()

class StatusUpdater(multiprocessing.Process):
    def __init__(self):
        super().__init__()
        self.queue = Queue()

    def run(self):
        connections.close_all()
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        while True:
            first_item = self.queue.get(block=True, timeout=None)
            updates = [first_item]
            waited = False
            while True:
                try: 
                    updates.append(self.queue.get(block=False))
                except queue.Empty:
                    if waited:
                        break
                    else:
                        time.sleep(1.0)
                        waited = True
            self.perform_updates(updates)
            if 'exit' in updates:
                break

        self._on_exit()
        logger.info(f"StatusUpdater thread finished.")
    
    def set_exit(self):
        self.queue.put('exit')
    
    def perform_updates(self, updates):
        raise NotImplementedError

    def _on_exit(self):
        pass
    
class BalsamDBStatusUpdater(StatusUpdater):
    def perform_updates(self, update_msgs):
        start_pks = []
        done_pks = []
        error_msgs = []

        for msg in update_msgs:
            if msg == 'exit': continue
            start_pks.extend(uuid.UUID(pk) for pk in msg['started']) # pk list
            done_pks.extend(uuid.UUID(pk) for pk in msg['done']) # pk list
            error_msgs.extend(msg['error']) # list: (pk, retcode, tail)

        if start_pks:
            BalsamJob.batch_update_state(start_pks, 'RUNNING')
            logger.info(f"StatusUpdater marked {len(start_pks)} RUNNING")
        if done_pks:
            BalsamJob.batch_update_state(done_pks, 'RUN_DONE', release=True)
            logger.info(f"StatusUpdater marked {len(done_pks)} DONE")
        if error_msgs:
            self._handle_errors(error_msgs)

    @transaction.atomic
    def _handle_errors(self, error_msgs):
        error_pks = [uuid.UUID(msg[0]) for msg in error_msgs]
        jobs = {
            job.pk: job
            for job in safe_select(BalsamJob.objects.filter(pk__in=error_pks))
        }
        for pk, retcode, tail in error_msgs:
            job = jobs[uuid.UUID(pk)]
            state_msg = f"nonzero return {retcode}: {tail}"
            job.update_state('RUN_ERROR', state_msg, release=True)

class JobSource(multiprocessing.Process):
    def __init__(self, prefetch_depth):
        super().__init__()
        self._exit_flag = multiprocessing.Event()
        self.queue = Queue()
        self.prefetch_depth = prefetch_depth
        try:
            self.queue.qsize()
        except NotImplementedError:
            monkeypatch_qsize(self.queue)

    def run(self):
        connections.close_all()
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        while not self._exit_flag.is_set():
            time.sleep(1)
            qsize = self.queue.qsize()
            fetch_count = max(0, self.prefetch_depth - qsize)
            logger.debug(f"JobSource queue depth is currently {qsize}. Fetching {fetch_count} more")
            if fetch_count:
                jobs = self._acquire_jobs(fetch_count)
                for job in jobs:
                    self.queue.put_nowait(job)
        self._on_exit()

    def get_jobs(self, max_count):
        fetched = []
        for i in range(max_count):
            try: fetched.append(self.queue.get_nowait())
            except queue.Empty: break
        return fetched

    def set_exit(self):
        self._exit_flag.set()

    def _acquire_jobs(self, num_jobs):
        raise NotImplementedError

    def _on_exit(self):
        pass

class BalsamJobSource(JobSource):
    def __init__(self, prefetch_depth, wf_filter):
        super().__init__(prefetch_depth)
        connections.close_all()
        self._manager = BalsamJob.source
        self._manager.workflow = wf_filter
        self._manager.start_tick()
        self._manager.clear_stale_locks()
        self._manager.check_qLaunch()
        connections.close_all()
        if wf_filter:
            logger.info(f'Pulling jobs with workflow matching: {wf_filter}')
        else:
            logger.info('No workflow filter. Consuming all jobs.')

    def _acquire_jobs(self, num_jobs):
        jobquery = self._manager.get_runnable(
            max_nodes=1,
            serial_only=True,
            order_by=('node_packing_count', # ascending
                      '-wall_time_minutes') # descending
        )
        jobs = {job.pk: job for job in jobquery[:num_jobs]}
        acquired_pks = self._manager.acquire(list(jobs.keys())) if jobs else []
        if acquired_pks:
            logger.info(f"BalsamJobSource acquired {len(acquired_pks)} jobs. Adding to Queue.")
        return [self._get_job_spec(jobs[pk]) for pk in acquired_pks]

    def _get_job_spec(self, job):
        return dict(
            pk=job.pk.hex,
            workdir=job.working_directory,
            name=job.name,
            cuteid=job.cute_id,
            cmd=job.app_cmd,
            occ=1.0 / job.node_packing_count,
            envs=job.get_envs(),
            envscript=job.envscript,
            required_num_cores=job.required_num_cores,
        )

    def _on_exit(self):
        timeout_pks = list(self._manager.filter(state="RUNNING").values_list("pk", flat=True))
        logger.info(f"Timing out {len(timeout_pks)} running jobs.")
        BalsamJob.batch_update_state(timeout_pks, "RUN_TIMEOUT", release=True)
        self._manager.release_all_owned()
        logger.info(f"BalsamJobSource thread finished.")

class ResourceManager:
    def __init__(self, job_source, status_updater):
        self.job_source = job_source
        self.status_updater = status_updater
        self.status_updater.start()
        self.job_source.start()

        self.recv_requests = {
            i: comm.irecv(MSG_BUFSIZE, source=i) for i in
            range(1, comm.size)
        }
        logger.info(f'Assigning jobs to {comm.size-1} worker ranks')
        
    def handle_requests(self):
        active = False
        reqs = [self.recv_requests[i] for i in range(1, comm.size)]
        statuses = []
        idxs, msgs = MPI.Request.testsome(reqs, statuses)

        for idx, msg, status in zip(idxs, msgs, statuses):
            logger.info(f"Rank {status.source} requested {msg['request_num_jobs']} jobs")
            assert status.source == idx + 1
            self.status_updater.queue.put_nowait(msg)
            sent_jobs = self.send_job_specs(
                max_jobs=msg['request_num_jobs'],
                dest_rank=status.source
            )

            active = active or msg.get("active", False) or sent_jobs

            self.recv_requests[status.source] = comm.irecv(
                MSG_BUFSIZE,
                source=status.source
            )
        return active

    def send_job_specs(self, max_jobs, dest_rank):
        new_job_specs = self.job_source.get_jobs(max_jobs)
        comm.send({'new_jobs': new_job_specs}, dest=dest_rank)
        logger.debug(f"Sent {len(new_job_specs)} new jobs to rank {dest_rank}")
        return len(new_job_specs) > 0

    def send_exit(self):
        for i in range(1, comm.size):
            comm.send({'exit': True}, dest=i)
        logger.info(f"Sent 'exit' message to all worker ranks.")

        logger.info(f"Terminating StatusUpdater...")
        # StatusUpdater needs to join first: stop the RUNNING updates
        self.status_updater.set_exit()
        self.status_updater.join()
        logger.info(f"StatusUpdater has joined.")

        # We trigger JobSource exit after StatusUpdater has joined to ensure
        # *all* Jobs get properly released and marked RUN_TIMEOUT
        logger.info(f"Terminating JobSource...")
        self.job_source.set_exit()
        self.job_source.join()
        logger.info(f"JobSource has joined.")

class Master:
    def __init__(self):
        self.MAX_IDLE_TIME = 120.0
        self.DELAY_PERIOD = 0.2
        self.idle_time = 0.0
        self.EXIT_FLAG = False

        args = self.parse_args()
        log_filename = config_logging('serial-launcher')
        bcast_msg = {
            "gpus_per_node": args.gpus_per_node,
            "worker_prefetch": args.worker_prefetch_count,
            "log_fname": log_filename,
        }
        comm.bcast(bcast_msg, root=0)
        self.remaining_timer = remaining_time_minutes(args.time_limit_min)
        next(self.remaining_timer)

        if args.db_prefetch_count == 0:
            prefetch = (comm.size - 1) * 128
        else:
            prefetch = args.db_prefetch_count

        job_source = BalsamJobSource(prefetch, args.wf_name)
        status_updater = BalsamDBStatusUpdater()
        self.manager = ResourceManager(job_source, status_updater)

    def parse_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--wf-name')
        parser.add_argument('--time-limit-min', type=float, default=72.*60)
        parser.add_argument('--gpus-per-node', type=int, default=0)
        parser.add_argument('--db-prefetch-count', type=int, default=0)
        parser.add_argument('--worker-prefetch-count', type=int, default=64)
        return parser.parse_args()

    def main(self):
        for remaining_minutes in self.remaining_timer:
            logger.debug(f"{remaining_minutes} minutes remaining")
            self._main()
            if self.EXIT_FLAG:
                logger.info("EXIT_FLAG on; master breaking main loop")
                break
            if self.idle_time > self.MAX_IDLE_TIME:
                logger.info(f"Nothing to do for {self.MAX_IDLE_TIME} seconds: quitting")
                break

        self.manager.send_exit()
        MPI.Finalize()
        logger.info(f"MPI Finalize done: ensemble master exit gracefully")

    def _main(self):
        have_active_workers = self.manager.handle_requests()
        if not have_active_workers:
            self.idle_time += self.DELAY_PERIOD
            logger.info(
                f"No active workers. Shutdown counter: {self.idle_time} / {self.MAX_IDLE_TIME}"
            )
        else:
            self.idle_time = 0.0
        time.sleep(self.DELAY_PERIOD)

class FailedToStartProcess:
    returncode = 12345
    def wait(self, timeout=0): return 12345
    def poll(self, timeout=0): return 12345
    def communicate(self, timeout=0): pass
    def terminate(self): pass
    def kill(self): pass

class WorkerRequest:

    def __init__(self):
        self._isend_req = None
        self._irecv_req = None
        self._message = {
            "started": [],
            "done": [],
            "error": [],
            "active": False,
            "request_num_jobs": 0,
        }

    def send(self):
        if self.in_flight: raise AttributeError("Request already in flight")
        self._isend_req = comm.isend(self._message, dest=0)
        self._irecv_req = comm.irecv(MSG_BUFSIZE, source=0)

    def set_active(self):
        if self._message['active']: return
        if self.in_flight: raise AttributeError("Request already in flight")
        self._message['active'] = True

    def add_started(self, pks_list):
        if self.in_flight: raise AttributeError("Request already in flight")
        self._message['started'].extend(pks_list)
    
    def add_done(self, pks_list):
        if self.in_flight: raise AttributeError("Request already in flight")
        self._message['done'].extend(pks_list)

    def add_error(self, error_list):
        if self.in_flight: raise AttributeError("Request already in flight")
        self._message['error'].extend(error_list)

    def set_request_num_jobs(self, num):
        if self.in_flight: raise AttributeError("Request already in flight")
        self._message['request_num_jobs'] = num

    def get_response(self):
        if not self.in_flight: raise AttributeError("Request has not been sent yet")
        was_received, response = self._irecv_req.test()
        if not was_received:
            return None
        return response

    @property
    def in_flight(self):
        return self._isend_req is not None

class Worker:
    CHECK_PERIOD=0.1
    RETRY_WINDOW = 20
    RETRY_CODES = [-11, 1, 255, 12345]
    MAX_RETRY = 3

    def __init__(self):
        self.processes = {}
        self.outfiles = {}
        self.cuteids = {}
        self.start_times = {}
        self.retry_counts = {}
        self.job_specs = {}
        self.runnable_cache = {}
        self.occupancy = 0.0
        self.all_affinity = [
            i*SERIAL_HYPERTHREAD_STRIDE
            for i in range(SERIAL_CORES_PER_NODE)
        ]
        self.used_affinity = []

    def _cleanup_proc(self, pk, timeout=0):
        self._kill(pk, timeout=timeout)
        self.processes[pk].communicate()
        self.outfiles[pk].close()
        self.occupancy -= self.job_specs[pk]["occ"]
        if self.occupancy <= 0.001:
            self.occupancy = 0.0
        # Get the affinity this job was using and free it in the internal list:
        for used_aff in self.job_specs[pk]['used_affinity']:
            self.used_affinity.remove(used_aff)
        for d in (self.processes, self.outfiles, self.cuteids, self.start_times,
                  self.retry_counts, self.job_specs):
            del d[pk]

    def _check_retcodes(self):
        pk_retcodes = []
        for pk, proc in self.processes.items():
            retcode = proc.poll()
            pk_retcodes.append((pk, retcode))
        return pk_retcodes

    def _log_error_tail(self, pk, retcode):
        fname = self.outfiles[pk].name
        if os.path.exists(fname):
            tail = get_tail(self.outfiles[pk].name)
        else:
            tail = ''
        logmsg = self.log_prefix(pk) + f'nonzero return {retcode}:\n {tail}'
        logger.error(logmsg)
        return tail

    def _can_retry(self, pk, retcode):
        if retcode in self.RETRY_CODES:
            elapsed = time.time() - self.start_times[pk]
            retry_count = self.retry_counts[pk]
            if elapsed < self.RETRY_WINDOW and retry_count <= self.MAX_RETRY:
                logmsg = self.log_prefix(pk)
                logmsg += (f'can retry task (err occured after {elapsed:.2f} sec; '
                          f'attempt {self.retry_counts[pk]}/{self.MAX_RETRY})')
                logger.error(logmsg)
                return True
        return False

    def _kill(self, pk, timeout=0):
        p = self.processes[pk]
        if p.poll() is None:
            p.terminate()
            logger.debug(f"rank {RANK} sent TERM to {self.cuteids[pk]}...waiting on shutdown")
            try: p.wait(timeout=timeout)
            except TimeoutExpired: p.kill()

    def _launch_proc(self, pk):
        job_spec = self.job_specs[pk]
        workdir = job_spec['workdir']
        name = job_spec['name']
        cmd = job_spec['cmd']
        envs = os.environ.copy()
        envs.update(job_spec['envs'])
        envscript = job_spec['envscript']
        required_num_cores = job_spec['required_num_cores']

        if envscript:
            args = ' '.join(['source', envscript, '&&', cmd])
            shell = True
        else:
            args = shlex.split(cmd)
            shell = False

        if self.gpus_per_node > 0:
            idx = list(self.job_specs.keys()).index(pk)
            gpu_device = idx % self.gpus_per_node
            envs['CUDA_DEVICE_ORDER'] = "PCI_BUS_ID"
            envs['CUDA_VISIBLE_DEVICES'] = str(gpu_device)

        # Set the affinity for this process:
        open_affinity = [ cpu for cpu in self.all_affinity if cpu not in self.used_affinity ]
        # Update the affinity:
        self.job_specs[pk]['used_affinity'] = open_affinity[0:required_num_cores]

        out_name = f'{name}.out'
        logger.info(f"{self.log_prefix(pk)} WORKER_START")
        logger.debug(f"{self.log_prefix(pk)} Popen (shell={shell}):\n{args}")

        if not os.path.exists(workdir): os.makedirs(workdir)
        outfile = open(os.path.join(workdir, out_name), 'wb')
        self.outfiles[pk] = outfile
        try:
            # Set this job's affinity:
            _p.cpu_affinity(self.job_specs[pk]['used_affinity'])
            proc = Popen(args, stdout=outfile, stderr=STDOUT,
                          cwd=workdir, env=envs, shell=shell,)
            # And, reset to all:
            _p.cpu_affinity([])
        except Exception as e:
            proc = FailedToStartProcess()
            logger.info(f"{self.log_prefix(pk)} WORKER_ERROR")
            logger.error(self.log_prefix(pk) + f"Popen error:\n{str(e)}\n")
            sleeptime = 0.5 + 3.5*random.random()
            time.sleep(sleeptime)
        # Update the list of used affinity after a successful launch:
        self.used_affinity += self.job_specs[pk]['used_affinity']
        self.processes[pk] = proc

    def _handle_error(self, pk, retcode):
        tail = self._log_error_tail(pk, retcode)

        if not self._can_retry(pk, retcode):
            self._cleanup_proc(pk)
            return (retcode, tail)
        else:
            self.outfiles[pk].close()
            self.start_times[pk] = time.time()
            self.retry_counts[pk] += 1
            self._launch_proc(pk)
            return 'running'

    def log_prefix(self, pk=None):
        prefix = f'rank {RANK} '
        if pk: prefix += f'{self.cuteids[pk]} '
        return prefix

    def poll_processes(self):
        done, error, active = [], [], False
        for pk, retcode in self._check_retcodes():
            active = True
            if retcode is None:
                continue
            elif retcode == 0:
                done.append(pk)
                logger.info(f"{self.log_prefix(pk)} WORKER_DONE")
                self._cleanup_proc(pk)
            else:
                logger.info(f"{self.log_prefix(pk)} WORKER_ERROR")
                status = self._handle_error(pk, retcode)
                if status != 'running':
                    retcode, tail = status
                    error.append((pk, retcode, tail))
        return done, error, active

    def exit(self):
        for pk in self.processes:
            self._cleanup_proc(pk, timeout=self.CHECK_PERIOD)
        MPI.Finalize()
        sys.exit(0)

    def start_jobs(self):
        started_pks = []

        for pk, job_spec in self.runnable_cache.items():
            if job_spec["occ"] + self.occupancy > 1.001:
                continue
            self.job_specs[pk] = job_spec
            self.cuteids[pk] = job_spec['cuteid']
            self.start_times[pk] = time.time()
            self.retry_counts[pk] = 1
            self._launch_proc(pk)
            started_pks.append(pk)
            self.occupancy += job_spec["occ"]

        self.runnable_cache = {
            k:v for k,v in self.runnable_cache.items()
            if k not in started_pks
        }
        return started_pks

    def main(self):
        bcast_msg = {}
        bcast_msg = comm.bcast(bcast_msg, root=0)
        self.gpus_per_node = bcast_msg["gpus_per_node"]
        self.prefetch_count = bcast_msg["worker_prefetch"]
        log_filename = bcast_msg["log_fname"]
        config_logging('serial-launcher', filename=log_filename)

        current_request, next_request = None, WorkerRequest()

        while True:
            done_pks, errors, active = self.poll_processes()
            started_pks = self.start_jobs()
            request_num_jobs = max(
                0,
                self.prefetch_count - len(self.runnable_cache)
            )

            next_request.add_started(started_pks)
            next_request.add_done(done_pks)
            next_request.add_error(errors)
            next_request.set_request_num_jobs(request_num_jobs)
            if active: next_request.set_active()

            if current_request is None:
                current_request = next_request
                next_request = WorkerRequest()
                current_request.send()
                response_msg = {}
            else:
                response_msg = current_request.get_response()
                if response_msg is not None:
                    current_request = None
                else:
                    time.sleep(0.2)
                    response_msg = {}

            if response_msg.get('exit', False):
                logger.debug(f"rank {RANK} received EXIT")
                break

            if response_msg.get('new_jobs'):
                self.runnable_cache.update({
                    job['pk']: job
                    for job in response_msg["new_jobs"]
                })

            logger.debug(
                f"rank {RANK} occupancy: {self.occupancy} "
                f"[{len(self.runnable_cache)} additional prefetched "
                f"jobs in cache]"
            )

        self.exit()

if __name__ == "__main__":
    if RANK == 0:
        master = Master()
        def handle_term(signum, stack): master.EXIT_FLAG = True
        signal.signal(signal.SIGINT, handle_term)
        signal.signal(signal.SIGTERM, handle_term)
        master.main()
    else:
        worker = Worker()
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        worker.main()
