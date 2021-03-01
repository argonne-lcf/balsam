from django.conf import settings
from balsam.service.schedulers import JobEnv
from balsam.launcher import mpi_commands
import socket
import logging
logger = logging.getLogger(__name__)


class Worker:
    def __init__(self, id, *, shape=None, block=None, corner=None,
                 num_nodes=None, host_type=None):
        self.id = id
        self.shape = shape
        self.block = block
        self.corner = corner
        self.num_nodes = num_nodes
        self.host_type = host_type
        self.idle = True

    @property
    def hostname(self):
        if self.host_type == 'COOLEY':
            return self.id.split('.')[0]
        if self.host_type != 'THETA':
            return self.id
        return f'nid{self.id:05d}'

    def __repr__(self):
        return f"worker{self.id}"


class WorkerGroup:
    '''Collection of Workers, constructed by passing in a specific host_type

    The host name and local batch scheduler's environment variables are used to
    identify the available compute resources and partition the resource into
    Workers.'''
    def __init__(self, limit=None, offset=None):
        self.host_type = settings.WORKER_DETECTION_TYPE
        self.workers_str = JobEnv.workers_str
        self.workers_file = JobEnv.workers_file
        self.workers = []

        try:
            self.setup = getattr(self, f"setup_{self.host_type}")
        except AttributeError as e:
            raise RuntimeError(
                "~/.balsam/settings.json contains an invalid value for "
                "WORKER_DETECTION_TYPE. Please choose a value that's "
                "listed in launcher/worker.py"
            ) from e

        self.setup()

        mpirun_class_name = settings.MPI_RUN_TEMPLATE
        try:
            mpirun_class = getattr(mpi_commands, mpirun_class_name)
        except AttributeError as e:
            raise RuntimeError(
                "~/.balsam/settings.json contains an invalid value for "
                "MPI_RUN_TEMPLATE.  Please choose a class name that's "
                " listed in launcher/mpi_commands.py"
            ) from e
        else:
            self.mpi_cmd = mpirun_class()

        # Apply limit and offset
        if limit is not None:
            if offset is None:
                self.workers = self.workers[:limit]
            else:
                self.workers = self.workers[offset:offset+limit]
        elif offset is not None:
                self.workers = self.workers[offset:]

        logger.info(f"Built {len(self.workers)} {self.host_type} workers")
        for worker in self.workers:
            worker.mpi_cmd = self.mpi_cmd
            logger.debug(f"ID {worker.id} NODES {worker.num_nodes}")

    def __iter__(self):
        return iter(self.workers)

    def __len__(self):
        return len(self.workers)

    def idle_workers(self):
        return [w for w in self.workers if w.idle]

    def request(self, num_nodes):
        idle_workers = self.idle_workers()
        assigned = []
        nodes_assigned = 0
        while nodes_assigned < num_nodes:
            if not idle_workers:
                break
            worker = idle_workers.pop()
            assigned.append(worker)
            nodes_assigned += worker.num_nodes
        if nodes_assigned < num_nodes:
            return []
        else:
            for worker in assigned:
                worker.idle = False
            return assigned

    def __getitem__(self, i):
        return self.workers[i]

    def setup_THETA(self):
        # workers_str is string like: 1001-1005,1030,1034-1200
        node_ids = []
        if not self.workers_str:
            raise ValueError("Theta WorkerGroup needs workers_str to setup")

        ranges = self.workers_str.split(',')
        for node_range in ranges:
            lo, *hi = node_range.split('-')
            lo = int(lo)
            if hi:
                hi = int(hi[0])
                node_ids.extend(list(range(lo, hi+1)))
            else:
                node_ids.append(lo)
        for id in node_ids:
            self.workers.append(Worker(id, host_type='THETA', num_nodes=1))

    def setup_SLURM(self):
        logger.info(f'SLURM_HOSTS: {self.workers_str}')
        if not self.workers_str:
            raise ValueError("Slurm WorkerGroup needs SLURM_HOSTS env")

        for id in self.workers_str.split():
            self.workers.append(
                Worker(id, host_type='SLURM', num_nodes=1)
            )

    def setup_BGQ(self):
        # Boot blocks
        # Get (block, corner, shape) args for each sub-block
        # For each worker, set num_nodes
        pass

    def setup_COOLEY(self):
        node_ids = []
        if not self.workers_file:
            raise ValueError("Cooley WorkerGroup needs workers_file to setup")

        data = open(self.workers_file).read()
        splitter = ',' if ',' in data else None
        node_ids = data.split(splitter)
        self.workers_str = " ".join(node_ids)

        for id in node_ids:
            self.workers.append(Worker(id, host_type='COOLEY', num_nodes=1))

    def setup_THETA_GPU(self):
        self.setup_COOLEY()

    def setup_DEFAULT(self):
        w = Worker(socket.gethostname(), host_type='DEFAULT', num_nodes=1)
        self.workers.append(w)
