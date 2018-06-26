'''mpi4py wrapper that allows an ensemble of serial applications to run in
parallel across ranks on the computing resource'''
import argparse
from collections import namedtuple
import os
import sys
import logging
import django
import random
import signal
import time
from socket import gethostname

os.environ['DJANGO_SETTINGS_MODULE'] = 'balsam.django_config.settings'
django.setup()
logger = logging.getLogger('balsam.launcher.mpi_ensemble')

from subprocess import Popen, STDOUT, TimeoutExpired

from mpi4py import MPI

from balsam.launcher import jobreader
from balsam.launcher.util import cd, get_tail, parse_real_time, remaining_time_minutes
from balsam.launcher.exceptions import *
from balsam.service.models import BalsamJob

comm = MPI.COMM_WORLD
RANK = comm.Get_rank()
django.db.connections.close_all()


class Tags:
    EXIT = 0  # master --> worker: exit now
    NEW = 1 # master --> worker: new job spec
    KILL = 2 # master --> worker: stop current job
    CONTINUE = 3 # master --> worker: keep running current job
    ASK = 4  # worker --> master: ask keep going?
    DONE = 5 # worker --> master: job success
    ERROR = 6 # worker --> master: job error


class ResourceManager:

    FETCH_PERIOD = 2.0
    KILLED_REFRESH_PERIOD = 3.0

    def __init__(self, host_names, job_source):
        self.host_names = host_names
        self.job_source = job_source
        self.node_occupancy = {name : 0.0 for name in set(host_names)}
        self.job_assignments = [None for i in range(comm.size)]
        self.job_assignments[0] = ('master', 1.0)

        self.last_job_fetch = -10.0
        self.last_killed_refresh = -10.0
        self.job_cache = []
        self.killed_pks = []

        self.host_rank_map = {}
        for name in set(host_names):
            self.host_rank_map[name] = [i for i,hostname in enumerate(host_names) if hostname == name]
            logger.debug(f"Hostname {name}: Ranks {self.host_rank_map[name]}")

        self.recv_requests = {}

    def refresh_job_cache(self, time_limit_min):
        now = time.time()
        if len(self.job_cache) == 0 or (now-self.last_job_fetch) > self.FETCH_PERIOD:
            jobquery = self.job_source.get_runnable(
                remaining_minutes=time_limit_min, 
                serial_only=True,
                order_by='-serial_node_packing_count' # descending
            )
            self.job_cache = list(jobquery)
            self.last_job_fetch = now
            job_str = '  '.join(j.cute_id for j in self.job_cache)
            logger.debug(f"Refreshed job cache: {len(self.job_cache)} runnable\n{job_str}")

    def refresh_killed_jobs(self):
        now = time.time()
        if now - self.last_killed_refresh > self.KILLED_REFRESH_PERIOD:
            killed_pks = self.job_source.filter(state='USER_KILLED').values_list('job_id', flat=True)

            if len(killed_pks) > len(self.killed_pks): 
                logger.debug(f"Killed jobs: {self.killed_pks}")
            self.killed_pks = killed_pks
            self.last_killed_refresh = now
        
    def _assign(self, rank, free_node, job):
        job_occ = 1.0 / job.serial_node_packing_count
        self.node_occupancy[free_node] += job_occ
        self.job_assignments[rank] = (job.pk, job_occ)
        mpiReq = self._send_job(job, rank)
        logger.debug(f"Sent {job.cute_id} to rank {rank} on {free_node}: occupancy is now {self.node_occupancy[free_node]}")
        return mpiReq

    def allocate_next_jobs(self, time_limit_min):
        '''Generator: yield (job,rank) pairs and mark the nodes/ranks as busy'''
        self.refresh_job_cache(time_limit_min)
        send_requests = []
        pre_assignments = []

        for job in self.job_cache:
            job_occ = 1.0 / job.serial_node_packing_count
            
            free_nodes = (name for name,occ in self.node_occupancy.items() if job_occ+occ < 1.001)
            free_nodes = sorted(free_nodes, key = lambda n: self.node_occupancy[n])
            free_ranks = ([i,node] for node in free_nodes for i in self.host_rank_map[node] if self.job_assignments[i] is None)
            assignment = next(free_ranks, None)

            if assignment is None:
                logger.debug(f'no free ranks to assign {job.cute_id}')
                break

            rank, free_node = assignment
            pre_assignments.append((rank, free_node, job))

        to_acquire = [job.pk for (rank, free_node, job) in pre_assignments]
        acquired_pks = self.job_source.acquire(to_acquire).values_list('job_id', flat=True)
        logger.info(f'Acquired lock on {len(acquired_pks)} out of {len(pre_assignments)} jobs marked for running')

        # Make actual assignment:
        for (rank, free_node, job) in pre_assignments:
            if job.pk in acquired_pks:
                mpiReq = self._assign(rank, free_node, job)
                send_requests.append(mpiReq)
            self.job_cache.remove(job)

        BalsamJob.batch_update_state(acquired_pks, 'RUNNING', 'submitted in MPI Ensemble')
        MPI.Request.waitall(send_requests)
        return len(send_requests) > 0

    def _send_job(self, job, rank):
        '''Send message to compute rank'''
        job_spec = {}

        job_spec['workdir'] = job.working_directory
        job_spec['name'] = job.name
        job_spec['cuteid'] = job.cute_id
        job_spec['cmd'] = job.app_cmd
        job_spec['envs'] = job.get_envs()

        req = comm.isend(job_spec, dest=rank, tag=Tags.NEW)
        self.recv_requests[rank] = comm.irecv(source=rank)
        return req

    def _get_requests(self):
        completed_requests = []
        stat = MPI.Status()
        for rank in self.recv_requests:
            req = self.recv_requests[rank]
            done, msg = req.test(status = stat)
            if done: 
                completed_requests.append((msg, stat.source, stat.tag))
                assert stat.source == rank

        for msg,rank,tag in completed_requests:
            del self.recv_requests[rank]
        return completed_requests

    def serve_requests(self):
        requests = self._get_requests()
        ask_reqs =   [rank for (msg,rank,tag) in requests if tag==Tags.ASK]
        done_reqs =  [rank for (msg,rank,tag) in requests if tag==Tags.DONE]
        error_reqs = [(msg, rank) for (msg,rank,tag) in requests if tag==Tags.ERROR]
        assert len(ask_reqs)+len(done_reqs)+len(error_reqs) == len(requests)

        if ask_reqs:   self._handle_asks(ask_reqs)
        if done_reqs:  self._handle_dones(done_reqs)
        if error_reqs: self._handle_errors(error_reqs)
        return len(requests)
        
    def _handle_asks(self, ask_ranks):
        self.refresh_killed_jobs()
        send_reqs = []
        kill_pks = []

        for rank in ask_ranks:
            pk, occ = self.job_assignments[rank]

            if pk in self.killed_pks:
                kill_pks.append(pk)
                req = comm.isend("kill", dest=rank, tag=Tags.KILL)
                self.job_assignments[rank] = None
                hostname = self.host_names[rank]
                self.node_occupancy[hostname] -= occ
                logger.debug(f"Sent KILL to rank {rank} on {hostname}: occupancy is now {self.node_occupancy[hostname]}")
            else:
                req = comm.isend("continue", dest=rank, tag=Tags.CONTINUE)
                self.recv_requests[rank] = comm.irecv(source=rank)
            send_reqs.append(req)

        if kill_pks: self.job_source.release(kill_pks)
        MPI.Request.waitall(send_reqs)
    
    def _handle_dones(self, done_ranks):
        TIMEstart = time.time()

        done_pks = []
        for rank in done_ranks:
            pk, occ = self.job_assignments[rank]
            done_pks.append(pk)
            
            self.job_assignments[rank] = None
            hostname = self.host_names[rank]
            self.node_occupancy[hostname] -= occ

        BalsamJob.batch_update_state(done_pks, 'RUN_DONE')
        self.job_source.release(done_pks)
        
        done_msg = ' '.join(str((pk,rank)) for pk,rank in zip(done_pks, done_ranks))
        logger.debug(f"RUN_DONE: {done_msg}")
        logger.debug(f"_handle_dones: {len(done_ranks)} handled in {time.time()-TIMEstart:.3f} seconds")
    
    def _handle_errors(self, error_reqs):
        TIMEstart = time.time()
        error_pks = []
        for msg,rank in error_reqs:
            pk, occ = self.job_assignments[rank]
            job = BalsamJob.objects.get(pk=pk)
            error_pks.append(pk)

            retcode, tail = msg['retcode'], msg['tail']
            state_msg = f"nonzero return {retcode}: {tail}"
            job.update_state('RUN_ERROR', state_msg)
            logger.error(f"{job.cute_id} RUN_ERROR from rank {rank}")
            logger.error(state_msg)

            self.job_assignments[rank] = None
            hostname = self.host_names[rank]
            self.node_occupancy[hostname] -= occ
        self.job_source.release(error_pks)
        logger.debug(f"_handle_errors: {len(error_reqs)} handled in {time.time()-TIMEstart:.3f} seconds")
    
    def send_exit(self):
        logger.info(f"send_exit: waiting on all pending recvs")
        requests = list(self.recv_requests.values())
        MPI.Request.waitall(requests)
        reqs = []
        logger.info(f"send_exit: send EXIT tag to all ranks")
        for i in range(1, comm.size):
            req = comm.isend('exit', dest=i, tag=Tags.EXIT)
            reqs.append(req)
        MPI.Request.waitall(reqs)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--wf-name')
    parser.add_argument('--time-limit-min', type=int, default=72*60)
    parser.add_argument('--gpus-per-node', type=int, default=0)
    return parser.parse_args()

def master_main(host_names):
    MAX_IDLE_TIME = 10.0
    DELAY_PERIOD = 1.0
    RUN_NEW_JOBS = True
    idle_time = 0.0
    EXIT_FLAG = False
    
    args = parse_args()
    job_source = BalsamJob.source
    job_source.workflow = args.wf_name
    job_source.start_tick()
    if job_source.workflow:
        logger.info(f'MPI Ensemble pulling jobs with WF {args.wf_name}')
    else:
        logger.info('MPI Ensemble consuming jobs matching any WF name')

    gpus_per_node = args.gpus_per_node
    time_limit_min = args.time_limit_min

    manager = ResourceManager(host_names, job_source)
    gpus_per_node = comm.bcast(gpus_per_node, root=0)

    transaction_context = django.db.transaction.atomic
    logger.info(f"Using real transactions: {transaction_context}")

    def handle_sigusr1(signum, stack):
        nonlocal RUN_NEW_JOBS
        RUN_NEW_JOBS = False

    def master_exit():
        manager.send_exit()
        logger.debug("Send_exit: master done")
        outstanding_job_pks = [j[0] for j in manager.job_assignments[1:] if j is not None]
        num_timeout = len(outstanding_job_pks)
        logger.info(f"Shutting down with {num_timeout} jobs still running..timing out")
        BalsamJob.batch_update_state(outstanding_job_pks, 'RUN_TIMEOUT', 'timed out in MPI Ensemble')

        manager.job_source.release_all_owned()
        logger.debug(f"master calling MPI Finalize")
        MPI.Finalize()
        logger.info(f"ensemble master exit gracefully")
        sys.exit(0)

    def handle_term(signum, stack):
        nonlocal EXIT_FLAG
        EXIT_FLAG = True
        logger.info(f"ensemble master got signal {signum}: switching on EXIT_FLAG")
        
    signal.signal(signal.SIGUSR1, handle_sigusr1)
    signal.signal(signal.SIGINT, handle_term)
    signal.signal(signal.SIGTERM, handle_term)

    remaining_timer = remaining_time_minutes(time_limit_min)
    next(remaining_timer)

    for remaining_minutes in remaining_timer:
        ran_anything = False
        got_requests = 0

        if RUN_NEW_JOBS:
            with transaction_context():
                ran_anything = manager.allocate_next_jobs(remaining_minutes)
        start = time.time()
        with transaction_context():
            got_requests = manager.serve_requests()
        elapsed = time.time() - start
        logger.debug(f"Served {got_requests} requests in {elapsed:.3f} seconds")

        if not (ran_anything or got_requests):
            time.sleep(DELAY_PERIOD)
            idle_time += DELAY_PERIOD
        else:
            idle_time = 0.0

        if idle_time > MAX_IDLE_TIME:
            if all(job == None for job in manager.job_assignments[1:]):
                logger.info(f"Nothing to do for {MAX_IDLE_TIME} seconds: quitting")
                break
        if EXIT_FLAG:
            break
    master_exit()


class Worker:
    CHECK_PERIOD=10

    def __init__(self):
        self.process = None
        self.outfile = None
        self.cuteid = None
    
    def wait_for_retcode(self):
        try: retcode = self.process.wait(timeout=self.CHECK_PERIOD)
        except TimeoutExpired:
            return None
        else: 
            self.process.communicate()
            self.process = None
            self.cuteid = None
            self.outfile.close()
            return retcode

    def kill(self):
        if self.process is None: return
        self.process.terminate()
        logger.debug(f"rank {RANK} sent TERM to {self.cuteid}...waiting on shutdown")
        try: self.process.wait(timeout=self.CHECK_PERIOD)
        except TimeoutExpired: self.process.kill()
        #logger.debug(f"Term done; closing out")
        self.process = None
        self.cuteid = None
        self.outfile.close()

    def worker_exit(self):
        #logger.debug(f"worker_exit")
        self.kill()
        #logger.debug(f"worker calling MPI Finalize")
        MPI.Finalize()
        #logger.debug(f"rank {RANK} worker_exit graceful")
        sys.exit(0)

    def start_job(self, job_dict):

        workdir = job_dict['workdir']
        name = job_dict['name']
        self.cuteid = job_dict['cuteid']
        cmd = job_dict['cmd']
        envs = job_dict['envs']

        if type(cmd) is str: cmd = cmd.split()

        # GPU: COOLEY SPECIFIC RIGHT NOW
        if self.gpus_per_node > 0:
            gpu_device = RANK % self.gpus_per_node  # TODO: generalize to multi-GPU system
            envs['CUDA_DEVICE_ORDER'] = "PCI_BUS_ID"
            envs['CUDA_VISIBLE_DEVICES'] = str(gpu_device)

        out_name = f'{name}.out'
        logger.debug(f"rank {RANK} {self.cuteid}\nPopen: {cmd}")

        if not os.path.exists(workdir): os.makedirs(workdir)
        os.chdir(workdir)
        self.outfile = open(out_name, 'wb')
        counter = 0
        while counter < 4:
            counter += 1
            try:
                self.process = Popen(cmd, stdout=self.outfile, stderr=STDOUT,
                                     cwd=workdir, env=envs, shell=False, bufsize=1,)
            except Exception as e:
                self.process = None
                logger.warning(f"rank {RANK} Popen error:\n{str(e)}\nRetrying Popen...")
                sleeptime = 0.5 + 3.5*random.random()
                time.sleep(sleeptime)
            else:
                return True

        if self.process is None:
            logger.error(f"Failed to Popen after 10 attempts; marking job as failed")
            return False

    def main(self):
        tag = None
        stat = MPI.Status()

        gpus_per_node = None
        self.gpus_per_node = comm.bcast(gpus_per_node, root=0)
        signal.signal(signal.SIGUSR1, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)

        while tag != Tags.EXIT:
            msg = comm.recv(source=0, status=stat)
            tag = stat.tag

            if tag == Tags.NEW:
                success = self.start_job(msg)
                if not success:
                    errMsg = {'retcode' : 123 , 'tail' : 'Could not Popen from mpi_ensemble'}
                    comm.send(errMsg, dest=0, tag=Tags.ERROR)
            elif tag == Tags.KILL:
                #logger.debug(f"rank {RANK} received KILL")
                self.kill()
            elif tag == Tags.EXIT:
                logger.debug(f"rank {RANK} received EXIT")
                self.kill()
                break

            if self.process:
                retcode = self.wait_for_retcode()
                if retcode is None:
                    message, requestTag = 'ask', Tags.ASK
                elif retcode == 0:
                    message, requestTag = 'done', Tags.DONE
                else:
                    tail = get_tail(self.outfile.name, nlines=10)
                    errMsg = {'retcode' : retcode, 'tail' : tail}
                    message, requestTag = errMsg, Tags.ERROR
                comm.send(message, dest=0, tag=requestTag)

        self.worker_exit()


if __name__ == "__main__":
    myname = MPI.Get_processor_name()
    host_names = comm.gather(myname, root=0)
    if RANK == 0:
        master_main(host_names)
    else:
        worker = Worker()
        worker.main()
