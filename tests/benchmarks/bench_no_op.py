import itertools
import os
import sys
from socket import gethostname
import time
import subprocess
from importlib.util import find_spec

from balsam.service.models import BalsamJob
from tests.BalsamTestCase import BalsamTestCase
from tests.BalsamTestCase import create_job, create_app

from tests import util

    
class TestNoOp(BalsamTestCase):

    def setUp(self):
        self.max_nodes = int(os.environ.get('COBALT_JOBSIZE', 1))
        num_nodes = [2**n for n in range(1,13) if 2**n <= self.max_nodes]
        rpn = [16, 32]
        jpn = [16, 64, 128, 256, 512, 1024]
        self.experiments = itertools.product(num_nodes, rpn, jpn)

    def serial_expt(self, num_nodes, rpn, jpn):
        BalsamJob.objects.all().delete()
        num_jobs = num_nodes * jpn
        
        for i in range(num_jobs):
            job = create_job(name=f'task{i}', direct_command=f'echo Hello',
                             args=str(i), workflow='bench-no-op')

    def test_no_op(self):
        for (num_nodes, rpn, jpn) in self.experiments:
            self.serial_expt(num_nodes, rpn, jpn)
