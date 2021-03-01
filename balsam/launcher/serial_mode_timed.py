import argparse
from collections import defaultdict
import os
import sys
import logging
import random
import subprocess

import multiprocessing
import queue
import shlex
import signal
import time
import uuid
import psutil
import socket
import zmq

_p = psutil.Process()
try:
    _p.cpu_affinity([])
    print("Detected psutil CPU affinity support.")
except AttributeError:
    class MockPsutilProcess:
        def cpu_affinity(self, list): pass
    _p = MockPsutilProcess()
    print("No psutil CPU Affinity support: will not bind processes to cores.")



from django.db import transaction, connections

from balsam import config_logging, setup
setup()
from balsam.launcher.util import get_tail, remaining_time_minutes
from balsam.core.models import BalsamJob, safe_select, PROCESSABLE_STATES
from django.conf import settings

# TODO(KGF): this is not available on Windows
multiprocessing.set_start_method("fork", force=True)

Queue = multiprocessing.Queue
try:
    Queue().qsize()
except NotImplementedError:
    from balsam.launcher.multi_queue_fallback import MyQueue
    Queue = MyQueue
    print("No queue.qsize support: will use fallback MyQueue implementation")


SERIAL_CORES_PER_NODE = settings.SERIAL_CORES_PER_NODE
SERIAL_HYPERTHREAD_STRIDE = settings.SERIAL_HYPERTHREAD_STRIDE
logger = logging.getLogger('balsam.launcher.zmq_ensemble')
connections.close_all()

class SectionTimer:
    _sections = {}
    total_elapsed = 0.0

    def __init__(self, name):
        self.name = name
        if name not in SectionTimer._sections:
            SectionTimer._sections[name] = []

    def __enter__(self):
        self.t0 = time.perf_counter()

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = time.perf_counter() - self.t0
        SectionTimer._sections[self.name].append(elapsed)
        SectionTimer.total_elapsed += elapsed
        if SectionTimer.total_elapsed > 300:
            self.report()

    @staticmethod
    def report():
        result = f'{"Section":24} {"MinTime":8} {"MaxTime":8} {"AvgTime":8} {"PctTime":5}\n'
        total_t = sum(sum(times) for times in SectionTimer._sections.values())
        for sec, times in SectionTimer._sections.items():
            min_t = min(times)
            max_t = max(times)
            avg_t = sum(times) / len(times)
            percent_t = 100 * sum(times) / total_t
            result += f'{sec:24} {min_t:8.3f} {max_t:8.3f} {avg_t:8.3f} {percent_t:5.1f}%\n'
        SectionTimer._sections = {}
        SectionTimer.total_elapsed = 0.0
        logger.debug("\n"+result)

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
        error_pks = []
        error_msgs = []

        early_start_pks = []

        for msg in update_msgs:
            if msg == 'exit': continue
            start_pks.extend(uuid.UUID(pk) for pk in msg['started']) # pk list
            done_pks.extend(uuid.UUID(pk) for pk in msg['done']) # pk list
            error_pks.extend(uuid.UUID(err[0]) for err in msg['error'])
            error_msgs.extend(msg['error']) # list: (pk, retcode, tail)

        for pk in done_pks:
            if pk in start_pks:
                start_pks.remove(pk)
                early_start_pks.append(pk)
        for pk in error_pks:
            if pk in start_pks:
                start_pks.remove(pk)
                early_start_pks.append(pk)

        if early_start_pks:
            BalsamJob.batch_update_state(early_start_pks, 'RUNNING')
            logger.info(f"StatusUpdater marked {len(start_pks)} RUNNING")
        if done_pks:
            BalsamJob.batch_update_state(done_pks, 'RUN_DONE', release=True)
            logger.info(f"StatusUpdater marked {len(done_pks)} DONE")
        if start_pks:
            BalsamJob.batch_update_state(start_pks, 'RUNNING')
            logger.info(f"StatusUpdater marked {len(start_pks)} RUNNING")
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
        self._manager.check_qLaunch()
        connections.close_all()
        self._started_tick = False
        if wf_filter:
            logger.info(f'Pulling jobs with workflow matching: {wf_filter}')
        else:
            logger.info('No workflow filter. Consuming all jobs.')

    def _acquire_jobs(self, num_jobs):
        if not self._started_tick:
            self._manager.clear_stale_locks()
            self._manager.start_tick()
            self._started_tick = True

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


class Master:
    def __init__(self, args):
        self.MAX_IDLE_TIME = 120.0
        self.DELAY_PERIOD = 0.2
        self.idle_time = None
        self.EXIT_FLAG = False
        self.num_workers = args.num_workers
        self.active_ids = set()
        self.is_persistent = args.persistent

        self.remaining_timer = remaining_time_minutes(args.time_limit_min)
        next(self.remaining_timer)

        if args.db_prefetch_count == 0:
            prefetch = self.num_workers * 96
        else:
            prefetch = args.db_prefetch_count

        logger.debug("Master creating source/status updater")
        self.job_source = BalsamJobSource(prefetch, args.wf_name)
        self.status_updater = BalsamDBStatusUpdater()
        self.status_updater.start()
        self.job_source.start()
        logger.debug("source/status updater created")

        logger.debug("Master ZMQ binding...")
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(f"tcp://*:{args.master_port}")
        logger.debug("Master ZMQ socket bound.")

    def handle_request(self):
        with SectionTimer("master_recv"):
            msg = self.socket.recv_json()
        with SectionTimer("master_enqueue_status"):
            self.status_updater.queue.put_nowait(msg)

        with SectionTimer("master_log_request"):
            finished_ids = set()
            for id in msg["done"]:
                finished_ids.add(id)
            for (id, retcode, tail) in msg["error"]:
                finished_ids.add(id)
            for id in msg["started"]:
                self.active_ids.add(id)
            self.active_ids -= finished_ids

            src = msg["source"]
            max_jobs = msg['request_num_jobs']
            logger.debug(f"Worker {src} requested {max_jobs} jobs")


        with SectionTimer("master_dequeue_jobs"):
            new_job_specs = self.job_source.get_jobs(max_jobs)
        with SectionTimer("master_send"):
            self.socket.send_json({'new_jobs': new_job_specs})
        if new_job_specs:
            with SectionTimer("master_log_new_jobs"):
                logger.debug(f"Sent {len(new_job_specs)} new jobs to {src}")

    def idle_check(self):
        if not self.active_ids:
            # self.idle_time marks the start of contiguous time without jobs for workers
            if self.idle_time is None:
                self.idle_time = time.time()
            # logger.debug(f"idle time started at {self.idle_time} seconds")
            # logger.debug(f"current time is {time.time()} seconds")
            # logger.debug(f"{time.time() - self.idle_time} seconds since start of idle time")
            if time.time() - self.idle_time > self.MAX_IDLE_TIME:
                logger.info(f"Nothing to do for {self.MAX_IDLE_TIME} seconds: quitting")
                self.EXIT_FLAG = True
        else:
            self.idle_time = None

    def main(self):
        logger.debug("In master main()")
        if not self.is_persistent:
            logger.debug(f"MAX_IDLE_TIME={self.MAX_IDLE_TIME} seconds")
        for remaining_minutes in self.remaining_timer:
            with SectionTimer("master_log_time"):
                logger.debug(f"{remaining_minutes} minutes remaining")
            self.handle_request()
            if not self.is_persistent:
                self.idle_check()
            if self.EXIT_FLAG:
                logger.info("EXIT_FLAG on; master breaking main loop")
                break
        self.shutdown()
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.close(linger=0)
        self.context.term()  # type: ignore
        logger.info(f"shutdown done: ensemble master exit gracefully")

    def shutdown(self):
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
        logger.info("Master sending exit message to all Workers")
        for _ in range(self.num_workers):
            self.socket.recv_json()
            self.socket.send_json({"exit": True})
        logger.info("All workers have received exit message. Quitting.")


class FailedToStartProcess:
    returncode = 12345
    def wait(self, timeout=0): return 12345
    def poll(self, timeout=0): return 12345
    def communicate(self, timeout=0): pass
    def terminate(self): pass
    def kill(self): pass


class Worker:
    CHECK_PERIOD=0.1
    RETRY_WINDOW = 20
    RETRY_CODES = [-11, 1, 255, 12345]
    MAX_RETRY = 3

    def __init__(self, args, hostname, master_subproc=None):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.master_address = f"tcp://{args.master_address}"
        self.hostname = hostname
        self.master_subproc=master_subproc
        self.EXIT_FLAG = False

        self.gpus_per_node = args.gpus_per_node
        self.prefetch_count = args.worker_prefetch_count
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
            with SectionTimer(f"{self.hostname}_poll"):
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
            logger.debug(f"worker {self.hostname} sent TERM to {self.cuteids[pk]}...waiting on shutdown")
            try: p.wait(timeout=timeout)
            except subprocess.TimeoutExpired: p.kill()

    def _launch_proc(self, pk):
        with SectionTimer(f'{self.hostname}_prep_job'):
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

        with SectionTimer(f'{self.hostname}_log_WORKER_START'):
            logger.debug(f"{self.log_prefix(pk)} WORKER_START")
        with SectionTimer(f'{self.hostname}_log_Popen'):
            logger.debug(f"{self.log_prefix(pk)} Popen (shell={shell}):\n{args}")

        with SectionTimer(f'{self.hostname}_mkdirs'):
            if not os.path.exists(workdir): os.makedirs(workdir)
        with SectionTimer(f'{self.hostname}_open_outfile'):
            outfile = open(os.path.join(workdir, out_name), 'wb')
            self.outfiles[pk] = outfile
        try:
            # Set this job's affinity:
            with SectionTimer(f'{self.hostname}_Popen'):
                _p.cpu_affinity(self.job_specs[pk]['used_affinity'])
                proc = subprocess.Popen(args, stdout=outfile, stderr=subprocess.STDOUT,
                                        cwd=workdir, env=envs, shell=shell,)
                # And, reset to all:
                _p.cpu_affinity([])
        except Exception as e:
            proc = FailedToStartProcess()
            logger.debug(f"{self.log_prefix(pk)} WORKER_ERROR")
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
        prefix = f'worker {self.hostname} '
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
                with SectionTimer(f'{self.hostname}_log_WORKER_DONE'):
                    logger.debug(f"{self.log_prefix(pk)} WORKER_DONE")
                with SectionTimer(f'{self.hostname}_cleanup_proc'):
                    self._cleanup_proc(pk)
            else:
                logger.info(f"{self.log_prefix(pk)} WORKER_ERROR")
                status = self._handle_error(pk, retcode)
                if status != 'running':
                    retcode, tail = status
                    error.append((pk, retcode, tail))
        return done, error, active

    def exit(self):
        pks = list(self.processes.keys())
        for pk in pks:
            self._cleanup_proc(pk, timeout=self.CHECK_PERIOD)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.close(linger=0)
        self.context.term()  # type: ignore
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
        logger.debug(f"Worker connecting to {self.master_address}")
        connections.close_all()
        self.socket.connect(self.master_address)
        logger.debug(f"Worker connected!")
        while True:
            done_pks, errors, active = self.poll_processes()
            started_pks = self.start_jobs()
            request_num_jobs = max(
                0,
                self.prefetch_count - len(self.runnable_cache)
            )

            msg = {
                "source": self.hostname,
                "started": started_pks,
                "done": done_pks,
                "error": errors,
                "active": active,
                "request_num_jobs": request_num_jobs,
            }
            with SectionTimer(f'{self.hostname}_send_recv'):
                self.socket.send_json(msg)
                logger.debug(f"Worker awaiting response...")
                response_msg = self.socket.recv_json()
                logger.debug(f"Worker response received")

            with SectionTimer(f'{self.hostname}_update_cache'):
                if response_msg.get('new_jobs'):
                    self.runnable_cache.update({
                        job['pk']: job
                        for job in response_msg["new_jobs"]
                    })
                if response_msg.get('exit'):
                    logger.info(f"Worker {self.hostname} received exit message: break")
                    self.exit()

            with SectionTimer(f'{self.hostname}_log_occ'):
                logger.debug(
                    f"{self.hostname} occupancy: {self.occupancy} "
                    f"[{len(self.runnable_cache)} additional prefetched "
                    f"jobs in cache]"
                )

            if self.EXIT_FLAG:
                if self.master_subproc is not None:
                    logger.info("Signal: forwarding SIGTERM to master subprocess.")
                    self.master_subproc.terminate()
                    logger.info("Signal: forwarded SIGTERM to master subprocess.")
            with SectionTimer(f'{self.hostname}_sleep1'):
                time.sleep(1)

        self.exit()


def launch_master_subprocess() -> "subprocess.Popen[bytes]":
    args = [sys.executable] + sys.argv + ["--run-master"]
    return subprocess.Popen(args)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--master-address', required=True)
    parser.add_argument('--log-filename', required=True)
    parser.add_argument('--num-workers', type=int, required=True)
    parser.add_argument('--wf-name')
    parser.add_argument('--run-master', action='store_true')
    parser.add_argument('--time-limit-min', type=float, default=72.*60)
    parser.add_argument('--gpus-per-node', type=int, default=0)
    parser.add_argument('--db-prefetch-count', type=int, default=0)
    parser.add_argument('--worker-prefetch-count', type=int, default=64)
    parser.add_argument('--persistent', action='store_true')
    args = parser.parse_args()
    args.master_host = args.master_address.split(':')[0]
    args.master_port = int(args.master_address.split(':')[1])
    return args

if __name__ == "__main__":
    args = parse_args()
    hostname = socket.gethostname()

    if args.run_master:
        log_fname = args.log_filename + ".master"
        config_logging(
            'serial-launcher',
            filename=log_fname,
            buffer_capacity=128,
        )
        master = Master(args)
        # TODO(KGF): factor out signal handling to SigHandler class
        # (util/sighandler.py) like in B2 1fc1824c
        def handle_term(signum, stack): master.EXIT_FLAG = True
        signal.signal(signal.SIGINT, handle_term)
        signal.signal(signal.SIGTERM, handle_term)
        master.main()
    else:
        log_fname = args.log_filename + "." + hostname
        config_logging(
            'serial-launcher',
            filename=log_fname,
            buffer_capacity=128,
        )
        if hostname == args.master_host:
            logger.debug(f"Worker starting Master on {args.master_address}")
            master_proc = launch_master_subprocess()
        else:
            master_proc = None
        worker = Worker(args, hostname=hostname, master_subproc=master_proc)
        def handle_term(signum, stack): worker.EXIT_FLAG = True
        signal.signal(signal.SIGINT, handle_term)
        signal.signal(signal.SIGTERM, handle_term)
        worker.main()
