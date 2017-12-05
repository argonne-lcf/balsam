from tests.BalsamTestCase import BalsamTestCase, cmdline

from balsam.schedulers import Scheduler
from balsam.models import BalsamJob, ApplicationDefinition

from balsamlauncher import jobreader
from balsamlauncher import worker
from balsamlauncher import runners
from balsamlauncher.launcher import get_args, create_new_runners

class TestRunners(BalsamTestCase):
    '''Integration test for WorkerGroup, JobReader, and Runners/RunnerGroup'''
    def setUp(self):
        self.scheduler = Scheduler.scheduler_main
        self.host_type = self.scheduler.host_type
        if self.host_type == 'DEFAULT':
            config = get_args('--consume-all --num-workers 4 --max-ranks-per-node 4'.split())
        else:
            config = get_args('--consume-all')

        self.worker_group = worker.WorkerGroup(config, host_type=self.host_type,
                                               workers_str=scheduler.workers_str)
        self.job_source = jobreader.JobReader.from_config(config)

    def testMPIEnsembleRunner(self):
        '''Several non-MPI jobs packaged into one mpi4py wrapper'''
        # Some jobs will pass; some will fail; some will timeout
        pass
    
    def testMPIRunner_passes(self):
        # varying ranks, rpn, tpr, tpc, envs
        # varying application args
        # check for successful job run, update, and output
        pass
    
    def testMPIRunner_fails(self):
        # ensure correct when job returns nonzero
        pass
    
    def testMPIRunner_timeouts(self):
        # ensure correct when longr-running job times out
        pass
    
    def test_create_runners(self):
        # Create sets of jobs intended to exercise each code path
        # in a single call to launcher.create_new_runners()
        pass
