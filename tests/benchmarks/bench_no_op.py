import os
import sys
from importlib.util import find_spec

from balsam.service.models import BalsamJob
from tests.BalsamTestCase import BalsamTestCase
from tests.BalsamTestCase import create_job, create_app

from tests import util

    
class TestNoOp(BalsamTestCase):

    def setUp(self):
        from itertools import takewhile, product

        self.launcherInfo = util.launcher_info()
        max_workers = self.launcherInfo.num_workers

        num_nodes = [2**n for n in range(1,13) if 2**n <= self.max_workers]
        if num_nodes[-1] != max_workers:
            num_nodes.append(max_workers)

        rpn = [64]
        jpn = [64, 256, 1024]
        self.experiments = itertools.product(num_nodes, rpn, jpn)

    def serial_expt(self, num_nodes, rpn, jpn):
        BalsamJob.objects.all().delete()
        num_jobs = num_nodes * jpn
        
        jobs = [create_job(name=f'task{i}', direct_command=f'echo Hello',
                         args=str(i), workflow='bench-no-op', save=False)
                         for i in range(num_jobs)]
        BalsamJob.objects.bulk_create(jobs)
        self.assertEqual(BalsamJob.objects.count(), num_jobs)

    def test_serial(self):
        for (num_nodes, rpn, jpn) in self.experiments:
            self.serial_expt(num_nodes, rpn, jpn)
