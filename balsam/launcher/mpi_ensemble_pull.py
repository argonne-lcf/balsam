'''mpi4py wrapper that allows an ensemble of serial applications to run in
parallel across ranks on the computing resource'''
from collections import namedtuple
import os
import sys
import logging
import django
import signal

os.environ['DJANGO_SETTINGS_MODULE'] = 'balsam.django_config.settings'
django.setup()
logger = logging.getLogger('balsam.launcher.mpi_ensemble')

from subprocess import Popen, STDOUT, TimeoutExpired

from mpi4py import MPI

from balsam.launcher.util import cd, get_tail, parse_real_time, remaining_time_minutes
from balsam.launcher.exceptions import *
from balsam.service.models import BalsamJob

comm = MPI.COMM_WORLD
RANK = comm.Get_rank()
HANDLE_EXIT = False
django.db.connections.close_all()


class Tags:
    EXIT = 0  # master --> worker: exit now
    NEW = 1 # master --> worker: new job spec
    KILL = 2 # master --> worker: stop current job
    CONTINUE = 3 # master --> worker: keep running current job
    ASK = 4  # worker --> master: ask for update
    DONE = 5 # worker --> master: job success
    ERROR = 6 # worker --> master: job error


class Config:
    def __init__(self, path):
        with open(sys.argv[1]) as fp: data = json.load(fp)

        job_file = data['job_file']
        wf_name = data['wf_name']
        self.time_limit_min = data['time_limit_min']

        if job_file:
            self.job_source = jobreader.FileJobReader(job_file)
        else:
            self.job_source = jobreader.WFJobReader(wf_name)


class ResourceManager:

    FETCH_PERIOD = 5.0
    KILLED_REFRESH_PERIOD = 10.0

    def __init__(self, host_names, job_source):
        self.host_names = host_names
        self.job_source = job_source
        self.node_occupancy = {name : 0.0 for name in set(host_names)}
        self.job_assignments = [None for i in range(comm.size)]
        self.job_assignments[0] = ('master', 1.0)

        self.last_job_fetch = -10.0
        self.last_killed_refresh = -10.0
        self.job_cache = []

        self.host_rank_map = {}
        for name in set(host_names):
            self.host_rank_map[name] = [i for i,hostname in enumerate(host_names) if hostname == name]

        self.recv_requests = {}

    def allocate_next_jobs(self, time_limit_min):
        '''Generator: yield (job,rank) pairs and mark the nodes/ranks as busy'''
        now = time.time()
        if not self.job_cache or now - self.last_job_fetch > self.FETCH_PERIOD:
            jobquery = self.job_source.get_runnable(time_limit_min, serial_only=True)
            jobs = jobquery.order_by('-serial_node_packing_count') # descending order
            self.job_cache = list(jobs)
            self.last_job_fetch = now

        submitted_indices = []
        send_requests = []
        for cache_idx, job in enumerate(self.job_cache):
            job_occ = 1.0 / job.serial_node_packing_count
            free_node = next((name for name,occ in self.node_occupancy.items() if job_occ+occ < 1.01), None)
            if free_node is None: break
            
            rank = next((i for i in self.host_rank_map[free_node] if self.job_assignments[i] is None), None)
            if rank is None: break

            self.node_occupancy[free_node] += job_occ
            self.job_assignments[rank] = (job.pk, job_occ)

            req = self._send_job(job, rank)
            send_requests.append(req)

            submitted_indices.append(cache_idx)
            logger.debug(f"Sent {job.cute_id} to rank {rank}")

        self.job_cache = [job for i, job in enumerate(self.job_cache)
                          if i not in submitted_indices]
        
        MPI.Request.waitall(send_requests)
        return len(submitted_indices) > 0

    
    def _send_job(self, job, rank):
        '''Send message to compute rank'''
        job.update_state('RUNNING', f'MPI Ensemble rank {rank}')

        job_spec = {}
        job_spec['workdir'] = job.working_directory
        job_spec['name'] = job.name
        job_spec['cuteid'] = job.cute_id
        job_spec['cmd'] = job.app_cmd
        job_spec['envs'] = job.get_envs()

        req = comm.isend(job_spec, dest=rank, tag=Tags.NEW)
        self.recv_requests[rank] = comm.irecv(source=rank)
        return req

    def serve_request(self):
        request = self._get_request()
        if request is not None:
            msg, source, tag = request
            self._handle_request(msg, source, tag)
            return True
        else:
            return False

    def send_exit(self):
        reqs = []
        for i in range(1, comm.size):
            req = comm.isend('exit', dest=i, tag=Tags.EXIT)
            reqs.append(req)
        MPI.Request.waitall(reqs)


    def _get_request(self):
        if not self.recv_requests: return None

        stat = MPI.Status()
        requests = list(self.recv_requests.values())
        index, msg = MPI.Request.waitany(requests, status=stat)
        source, tag = stat.source, stat.tag
        del self.recv_requests[source]
        return msg, source, tag

    def _handle_request(self, msg, source, tag):
        if tag == Tags.ASK:
            self._handle_ask(msg, source, tag)
        elif tag == Tags.DONE:
            self._handle_done(msg, source, tag)
        elif tag == Tags.ERROR:
            self._handle_error(msg, source, tag)
        else:
            raise RuntimeError(f"Unexpected tag from worker: {tag}")


    def _handle_ask(self, msg, source, tag):
        now = time.time()
        if now - self.last_killed_refresh > self.KILLED_REFRESH_PERIOD:
            self.killed_pks = list(
                self.job_source.by_states('USER_KILLED').values_list('pk', flat=True)
            )
            self.last_killed_refresh = now

        pk, occ = self.job_assignments[source]

        if pk in self.killed_pks:
            req = comm.isend("kill", dest=source, tag=Tags.KILL)
            self.job_assignments[source] = None
            hostname = self.host_names[source]
            self.node_occupancy[hostname] -= occ
        else:
            req = comm.isend("continue", dest=source, tag=Tags.CONTINUE)
        req.wait()
    
    def _handle_done(self, msg, source, tag):
        pk, occ = self.job_assignments[source]

        job = BalsamJob.objects.get(pk=pk)
        job.update_state('RUN_DONE')
        elapsed_seconds = msg['elapsed_seconds']
        if elapsed_seconds:
            job.runtime_seconds = float(elapsed_seconds)
            job.save(update_fields = ['runtime_seconds'])

        self.job_assignments[source] = None
        hostname = self.host_names[source]
        self.node_occupancy[hostname] -= occ
    
    def _handle_error(self, msg, source, tag):
        pk, occ = self.job_assignments[source]

        job = BalsamJob.objects.get(pk=pk)
        retcode, tail = msg['retcode'], msg['tail']
        state_msg = f"nonzero return {retcode}: {tail}"
        job.update_state('RUN_ERROR', state_msg)

        self.job_assignments[source] = None
        hostname = self.host_names[source]
        self.node_occupancy[hostname] -= occ
        
def master_main(host_names):

    MAX_IDLE_TIME = 5.0
    DELAY_PERIOD = 1.0
    RUN_NEW_JOBS = True
    idle_time = 0.0

    def handle_sigusr1(signum, stack):
        nonlocal RUN_NEW_JOBS
        RUN_NEW_JOBS = False

    def handle_term(signum, stack):
        nonlocal manager
        manager.send_exit()
        MPI.Finalize()
        sys.exit(0)
        

    signal.signal(signal.SIGUSR1, handle_sigusr1)


    config = Config(sys.argv[1])
    job_source = config.job_source
    time_limit_min = config.time_limit_min
    manager = ResourceManager(host_names, job_source)

    remaining_timer = remaining_time_minutes(time_limit_min)
    next(remaining_timer)

    for remaining_minutes in remaining_timer:
        no_wait = False

        if RUN_NEW_JOBS:
            no_wait = manager.allocate_next_jobs(remaining_minutes)
        no_wait = manager.serve_request()

        if no_wait == False:
            time.sleep(DELAY_PERIOD)
            idle_time += DELAY_PERIOD
        else:
            idle_time = 0.0

        if idle_time > MAX_IDLE_TIME:
            if all(job == None for job in manager.job_assignments[1:]):
                manager.send_exit()
                break

class Worker:
    CHECK_PERIOD=10

    def __init__(self):
        self.process = None
        self.outfile = None
    
    def wait_for_retcode(self):
        try: retcode = self.process.wait(timeout=self.CHECK_PERIOD)
        except TimeoutExpired: 
            return None
        else: 
            self.process = None
            self.outfile.close()
            return retcode

    def kill(self):
        if self.process is None: return
        self.process.terminate()
        try: self.process.wait(timeout=CHECK_PERIOD)
        except TimeoutExpired: self.process.kill()
        self.process = None
        self.outfile.close()

    def on_exit(self):
        self.kill()
        MPI.Finalize()
        sys.exit(0)

    def start_job(self, job_dict):
        workdir = job_dict['workdir']
        name = job_dict['name']
        cuteid = job_dict['cuteid']
        cmd = job_dict['cmd']
        envs = job_dict['envs']

        out_name = f'{name}.out'
        logger.debug(f"rank {RANK} {cuteid} {cmd}")

        os.chdir(workdir)
        self.outfile = open(out_name, 'wb')
        try:
            self.process = Popen(cmd, stdout=self.outfile, stderr=STDOUT,
                                 cwd=workdir, env=envs, shell=True, 
                                 executable='/bin/bash')
        except Exception as e:
            logger.exception(f"Popen error:\n{str(e)}")
            raise

    def main(self):
        tag = None
        stat = MPI.Status()

        handler = lambda a,b : self.on_exit()
        signal.signal(signal.SIGUSR1, signal.SIG_IGN)
        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

        while tag != Tags.EXIT:
            msg = comm.recv(source=0, status=stat)
            tag = stat.tag

            if tag == Tags.NEW:
                self.start_job(msg)
            elif tag == Tags.KILL:
                self.kill()
            elif tag = Tags.EXIT:
                self.kill()
                break

            if self.process:
                retcode = self.wait_for_retcode()
                if retcode is None:
                    message, requestTag = 'ask', Tags.ASK
                elif retcode == 0:
                    elapsed = parse_real_time(get_tail(self.outfile.name, indent=''))
                    doneMsg = {'elapsed_seconds' : elapsed}
                    message, requestTag = doneMsg, Tags.DONE
                else:
                    tail = get_tail(self.outfile.name, nlines=10)
                    errMsg = {'retcode' : retcode, 'message' : tail}
                    message, requestTag = errMsg, Tags.ERROR
                comm.send(message, dest=0, tag=requestTag)
        self.on_exit()


if __name__ == "__main__":
    myname = MPI.Get_processor_name()
    host_names = comm.gather(myname, root=0)
    if RANK == 0:
        master_main(host_names)
    else:
        worker = Worker()
        worker.main()
