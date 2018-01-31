'''Worker: Abstraction for the compute unit running a job.
Cray: 1 worker = 1 node
BG/Q: 1 worker = 1 subblock
Default: 1 worker = local host machine

Workers contain any identifying information needed to assign jobs to specific
workers (e.g. via "mpirun") and the WorkerGroup keeps track of all busy and idle
workers available in the current launcher instance'''

import logging
logger = logging.getLogger(__name__)
class Worker:
    def __init__(self, id, *, shape=None, block=None, corner=None,
                 num_nodes=None, max_ranks_per_node=None, host_type=None):
        self.id = id
        self.shape = shape
        self.block = block
        self.corner = corner
        self.num_nodes = num_nodes
        self.max_ranks_per_node = max_ranks_per_node
        self.host_type = host_type
        self.idle = True

class WorkerGroup:
    '''Collection of Workers, constructed by passing in a specific host_type
    
    The host name and local batch scheduler's environment variables are used to
    identify the available compute resources and partition the resource into
    Workers.'''
    def __init__(self, config, *, host_type=None, workers_str=None,
                 workers_file=None):
        '''Initialize WorkerGroup
        
        Args:
            - ``host_type``: one of CRAY, BGQ, COOLEY, DEFAULT
            - ``workers_str``: system-specific string identifying compute
              resources
            - ``workers_file``: system-specific file identifying compute
              resources
        '''
        self.host_type = host_type
        self.workers_str = workers_str
        self.workers_file = workers_file
        self.workers = []
        self.setup = getattr(self, f"setup_{self.host_type}")
        self.setup(config)

        if config.num_workers >= 1:
            self.workers = self.workers[:config.num_workers]

        logger.info(f"Built {len(self.workers)} {self.host_type} workers")
        for worker in self.workers:
            logger.debug(
                f"ID {worker.id} NODES {worker.num_nodes} MAX-RANKS-PER-NODE"
                f" {worker.max_ranks_per_node}"
            )

    def __iter__(self):
        return iter(self.workers)

    def __len__(self):
        return len(self.workers)

    def __getitem__(self, i):
        return self.workers[i]

    def setup_CRAY(self, config):
        # workers_str is string like: 1001-1005,1030,1034-1200
        node_ids = []
        if not self.workers_str:
            raise ValueError("Cray WorkerGroup needs workers_str to setup")
            
        serial_rpn = config.max_ranks_per_node
        if serial_rpn <= 1: serial_rpn = 16

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
            self.workers.append(Worker(id, host_type='CRAY',
                                num_nodes=1, max_ranks_per_node=serial_rpn))

    def setup_BGQ(self, config):
        # Boot blocks
        # Get (block, corner, shape) args for each sub-block
        # For each worker, set num_nodes and max_ranks_per_node attributes
        pass

    def setup_COOLEY(self, config):
        node_ids = []
        if not self.workers_file:
            raise ValueError("Cooley WorkerGroup needs workers_file to setup")
        
        data = open(self.workers_file).read()
        splitter = ',' if ',' in data else None
        node_ids = data.split(splitter)
        self.workers_str = " ".join(node_ids)
        
        serial_rpn = config.max_ranks_per_node
        if serial_rpn <= 1: serial_rpn = 16

        for id in node_ids:
            self.workers.append(Worker(id, host_type='COOLEY', num_nodes=1,
                                       max_ranks_per_node=serial_rpn))

    def setup_DEFAULT(self, config):
        # Use command line config: num_workers, nodes_per_worker,
        # max_ranks_per_node
        num_workers = config.num_workers
        if not num_workers or num_workers < 1:
            num_workers = 1
        for i in range(num_workers):
            w = Worker(i, host_type='DEFAULT',
                       num_nodes=config.nodes_per_worker,
                       max_ranks_per_node=config.max_ranks_per_node)
            self.workers.append(w)
