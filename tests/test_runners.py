from collections import namedtuple
import os
import sys
import time
from importlib.util import find_spec
from tests.BalsamTestCase import BalsamTestCase, cmdline

from django.conf import settings

from balsam.schedulers import Scheduler
from balsam.models import BalsamJob, ApplicationDefinition

from balsamlauncher import jobreader
from balsamlauncher import worker
from balsamlauncher import runners
from balsamlauncher.launcher import get_args, create_new_runners

class TestMPIRunner(BalsamTestCase):
    '''start, update_jobs, finished, error/timeout handling'''
    def setUp(self):
        scheduler = Scheduler.scheduler_main
        self.host_type = scheduler.host_type
        if self.host_type == 'DEFAULT':
            config = get_args('--consume-all --num-workers 1 --max-ranks-per-node 4'.split())
        else:
            config = get_args('--consume-all')

        self.worker_group = worker.WorkerGroup(config, host_type=self.host_type,
                                               workers_str=scheduler.workers_str,
                                               workers_file=scheduler.workers_file)
        self.job_source = jobreader.JobReader.from_config(config)

        app_path = f"{sys.executable}  {find_spec('tests.mock_mpi_app').origin}"
        self.app = ApplicationDefinition()
        self.app.name = "mock_mpi"
        self.app.description = "print and sleep"
        self.app.executable = app_path
        self.app.save()

    
    def assert_output_file_contains_n_ranks(self, fp, n):
        '''specific check of mock_mpi_app.py output'''
        found = []
        for line in fp:
            found.append(int(line.split()[1]))
        self.assertSetEqual(set(range(n)), set(found))

    def testMPIRunner_passes(self):
        # Test various worker configurations:
        work_configs = []
        WorkerConfig = namedtuple('WorkerConfig', ['workers', 'num_nodes',
                                                   'ranks_per_node'])
        # 2 ranks on one node
        node0 = self.worker_group[0]
        cfg = WorkerConfig([node0], 1, 2)
        work_configs.append(cfg)
        
        # max ranks on one node
        cfg = WorkerConfig([node0], 1, node0.max_ranks_per_node)
        work_configs.append(cfg)
        
        # max ranks on all nodes
        cfg = WorkerConfig(list(self.worker_group), len(self.worker_group),
                           node0.max_ranks_per_node)
        work_configs.append(cfg)

        for i, (workerslist, num_nodes, rpn) in enumerate(work_configs):
            job = BalsamJob()
            job.name = f"test{i}"
            job.application = "mock_mpi"
            job.allowed_work_sites = settings.BALSAM_SITE
            job.num_nodes = num_nodes
            job.ranks_per_node = rpn
            job.save()
            self.assertEquals(job.state, 'CREATED')
            job.create_working_path()

            runner = runners.MPIRunner([job], workerslist)
            runner.start()
            runner.update_jobs()
            while not runner.finished():
                self.assertEquals(job.state, 'RUNNING')
                runner.update_jobs()
                time.sleep(0.5)
            runner.update_jobs()
            self.assertEquals(job.state, 'RUN_DONE')

            outpath = runner.outfile.name
            with open(outpath) as fp:
                self.assert_output_file_contains_n_ranks(fp, num_nodes*rpn)

    
    def testMPIRunner_fails(self):
        # ensure correct when job returns nonzero
        work_configs = []
        WorkerConfig = namedtuple('WorkerConfig', ['workers', 'num_nodes',
                                                   'ranks_per_node'])
        # 2 ranks on one node
        node0 = self.worker_group[0]
        cfg = WorkerConfig([node0], 1, 2)
        work_configs.append(cfg)
        
        # max ranks on one node
        cfg = WorkerConfig([node0], 1, node0.max_ranks_per_node)
        work_configs.append(cfg)
        
        # max ranks on all nodes
        cfg = WorkerConfig(list(self.worker_group), len(self.worker_group),
                           node0.max_ranks_per_node)
        work_configs.append(cfg)

        for i, (workerslist, num_nodes, rpn) in enumerate(work_configs):
            job = BalsamJob()
            job.name = f"test{i}"
            job.application = "mock_mpi"
            job.allowed_work_sites = settings.BALSAM_SITE
            job.num_nodes = num_nodes
            job.ranks_per_node = rpn
            job.application_args = '--retcode 255'
            job.save()
            self.assertEquals(job.state, 'CREATED')
            job.create_working_path()

            workers = self.worker_group[0]
            runner = runners.MPIRunner([job], workerslist)
            runner.start()
            runner.update_jobs()
            while not runner.finished():
                self.assertEquals(job.state, 'RUNNING')
                runner.update_jobs()
                time.sleep(0.5)
            runner.update_jobs()
            self.assertEquals(job.state, 'RUN_ERROR')
    
    def testMPIRunner_timeouts(self):
        # ensure correct when longr-running job times out
        pass
    
    
class TestMPIEnsemble:
    def setUp(self):
        pass

    def testMPIEnsembleRunner(self):
        '''Several non-MPI jobs packaged into one mpi4py wrapper'''
        # Some jobs will pass; some will fail; some will timeout
        pass


class TestRunnerGroup:
    def setUp(self):
        pass

    def test_create_runners(self):
        # Create sets of jobs intended to exercise each code path
        # in a single call to launcher.create_new_runners()
        pass
