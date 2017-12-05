from tests.BalsamTestCase import BalsamTestCase

from balsamlauncher import worker
from balsamlauncher.launcher import get_args
from balsam.schedulers import Scheduler


class WorkerGroupUnitTests(BalsamTestCase):
    def setUp(self):
        self.scheduler = Scheduler.scheduler_main

    def test_default(self):
        '''Create default worker groups with various command line arguments'''
        
        config = get_args('--consume-all --num-workers 1'.split())
        group = worker.WorkerGroup(config, host_type='DEFAULT', workers_str=None)
        self.assertEqual(len(group.workers), 1)
        self.assertEqual(group.workers[0].num_nodes, 1)
        self.assertEqual(group.workers[0].max_ranks_per_node, 1)
        
        config = get_args('--consume-all --num-workers 3 --max-ranks-per-node 4'.split())
        group = worker.WorkerGroup(config, host_type='DEFAULT', workers_str=None)
        self.assertEqual(len(group.workers), 3)
        self.assertEqual(group.workers[0].num_nodes, 1)
        self.assertEqual(group.workers[0].max_ranks_per_node, 4)

    def test_cray(self):
        '''Construct WorkerGroup from reading Cray environment'''
        config = get_args('--consume-all'.split())
        if self.scheduler.host_type != 'CRAY':
            self.skipTest('scheduler did not recognize Cray environment')
        group = worker.WorkerGroup(config, host_type='CRAY', 
                                   workers_str=self.scheduler.workers_str)
        if self.scheduler.workers_str:
            num_worker_env = self.scheduler.SCHEDULER_VARIABLES['num_workers']
            self.assertEqual(len(group.workers), int(os.environ[num_worker_env]))
