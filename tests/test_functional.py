from collections import namedtuple
import os
import random
from multiprocessing import Lock
import sys
import time
from uuid import UUID
from importlib.util import find_spec
from tests.BalsamTestCase import BalsamTestCase, cmdline
from tests.BalsamTestCase import poll_until_returns_true
from tests.BalsamTestCase import create_job, create_app

from django.conf import settings

from balsam.schedulers import Scheduler
from balsam.models import BalsamJob

from balsamlauncher import worker
from balsamlauncher import runners
from balsamlauncher.launcher import get_args, create_new_runners

class TestSingleJobTransitions(BalsamTestCase):
    def setUp(self):
        scheduler = Scheduler.scheduler_main
        self.host_type = scheduler.host_type
        if self.host_type == 'DEFAULT':
            config = get_args('--consume-all --num-workers 1 --max-ranks-per-node 4'.split())
        else:
            config = get_args('--consume-all'.split())

        self.worker_group = worker.WorkerGroup(config, host_type=self.host_type,
                                               workers_str=scheduler.workers_str,
                                               workers_file=scheduler.workers_file)

        app_path = f"{sys.executable}  {find_spec('tests.mock_serial_app').origin}"
        self.app = create_app(name="mock_serial", executable=app_path,
                              preproc='', postproc='', envs={})

    def test_one_job_normal(self):
        job = create_job(name='test', app=self.app.name)
