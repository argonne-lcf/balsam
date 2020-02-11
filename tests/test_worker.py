import os
import sys
from importlib.util import find_spec

from tests.BalsamTestCase import BalsamTestCase

from balsam.launcher import worker
from balsam.launcher.launcher import get_args
from balsam.service.schedulers import Scheduler

from tests import util


class WorkerGroupUnitTests(BalsamTestCase):
    def setUp(self):
        self.scheduler = Scheduler.scheduler_main

    def test_default(self):
        '''Create default worker groups with various command line arguments'''

        config = get_args('--consume-all --num-workers 1'.split())
        group = worker.WorkerGroup(config, host_type='DEFAULT', workers_str=None)
        self.assertEqual(len(group.workers), 1)
        self.assertEqual(group.workers[0].num_nodes, 1)
        self.assertEqual(group.workers[0].max_ranks_per_node, 4)

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
                                   workers_str=self.scheduler.workers_str,
                                   workers_file=self.scheduler.workers_file)
        if self.scheduler.workers_str:
            num_worker_env = self.scheduler.SCHEDULER_VARIABLES['num_workers']
            self.assertEqual(len(group.workers), int(os.environ[num_worker_env]))

    def test_cooley(self):
        '''Construct WorkerGroup from reading Cooley environment'''
        config = get_args('--consume-all'.split())
        if self.scheduler.host_type != 'COOLEY':
            self.skipTest('scheduler did not recognize Cooley environment')
        group = worker.WorkerGroup(config, host_type='COOLEY',
                                   workers_str=self.scheduler.workers_str,
                                   workers_file=self.scheduler.workers_file)
        self.assertGreaterEqual(len(group.workers), 1)

    def test_mpi_can_run(self):
        '''The system-detected mpirun works'''
        launchInfo = util.launcher_info()
        worker_group = launchInfo.workerGroup
        mpi_cmd = launchInfo.mpi_cmd

        app_path = f"{sys.executable}  {find_spec('tests.mock_mpi_app').origin}"
        mpi_str = mpi_cmd([worker_group[0]], app_cmd=app_path, envs={},
                          num_ranks=2, ranks_per_node=2,
                          threads_per_rank=1, threads_per_core=1)
        stdout, _ = util.cmdline(mpi_str)
        self.assertIn('Rank 0', stdout)
        self.assertIn('Rank 1', stdout)
