'''Worker: Abstraction for the compute unit running a job.
Cray: 1 worker = 1 node
BG/Q: 1 worker = 1 subblock
Default: 1 worker = local host machine

Workers contain any identifying information needed to assign jobs to specific
workers (e.g. via "mpirun") and the WorkerGroup keeps track of all busy and idle
workers available in the current launcher instance'''
class Worker:
    def __init__(self, id, *, shape=None, block=None, corner=None,
                 ranks_per_worker=None, host_type=None):
        self.id = id
        self.shape = shape
        self.block = block
        self.corner = corner
        self.ranks_per_worker = ranks_per_worker
        self.host_type = host_type
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
        # partition is string like: 1001-1005,1030,1034-1200
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
            self.workers.append(Worker(id, host_type='CRAY'))

    def setup_BGQ(self):
        # Boot blocks
        # Get (block, corner, shape) args for each sub-block
        pass

    def setup_DEFAULT(self):
        for i in range(self.num_workers):
            self.workers.apppend(Worker(i), host_type='DEFAULT')
