import datetime
import os
import pprint
import re
import sys
from importlib.util import find_spec

from balsam.service.models import BalsamJob, TIME_FMT
from tests.BalsamTestCase import BalsamTestCase
from tests.BalsamTestCase import create_job, create_app

from tests import util
    
def state_hist_pattern(state):
    return re.compile(f'''
    ^                  # start of line
    \[                 # opening square bracket
    (\d+-\d+-\d\d\d\d  # date MM-DD-YYYY
    \s+                # one or more space
    \d+:\d+:\d+)       # time HH:MM:SS
    \s+                # one or more space
    {state}            # state
    \s*                # 0 or more space
    \]                 # closing square bracket
    ''', 
    re.VERBOSE | re.MULTILINE
    )

p_ready = state_hist_pattern('READY')
p_pre = state_hist_pattern('PREPROCESSED')
p_rundone = state_hist_pattern('RUN_DONE')
p_finished = state_hist_pattern('JOB_FINISHED')
    
class TestNoOp(BalsamTestCase):

    def setUp(self):
        from itertools import takewhile, product

        self.launcherInfo = util.launcher_info()
        max_workers = self.launcherInfo.num_workers

        num_nodes = [2**n for n in range(1,13) if 2**n <= self.max_workers]
        if num_nodes[-1] != max_workers:
            num_nodes.append(max_workers)

        #rpn = [64]
        #jpn = [64, 256, 1024]
        rpn = [16]
        jpn = [64, 128]
        self.experiments = itertools.product(num_nodes, rpn, jpn)

    def create_serial_expt(self, num_nodes, rpn, jpn):
        BalsamJob.objects.all().delete()
        num_jobs = num_nodes * jpn
        
        jobs = [create_job(name=f'task{i}', direct_command=f'echo Hello',
                         args=str(i), workflow='bench-no-op', save=False)
                         for i in range(num_jobs)]
        BalsamJob.objects.bulk_create(jobs)
        self.assertEqual(BalsamJob.objects.count(), num_jobs)

    def process_job_times(self):
        state_data = BalsamJob.objects.values_list('state_history', flat=True)

        ready_times = (p_ready.search(jobhist).group(1) for jobhist in statedata)
        ready_times = [datetime.strptime(time_str, TIME_FMT) for time_str in ready_times]
        time0 = min(ready_times)
        ready_times = [(t - time0).seconds for t in ready_times]
        print("Ready Times")
        pprint(ready_times)
        
        finished_times = (p_finished.search(jobhist).group(1) for jobhist in statedata)
        finished_times = [datetime.strptime(time_str, TIME_FMT) for time_str in finished_times]
        finished_times = [(t - time0).seconds for t in finished_times]
        print("Finished Times")
        pprint(finished_times)

    def test_serial(self):
        done_query = BalsamJob.objects.filter(state='JOB_FINISHED')
        for (num_nodes, rpn, jpn) in self.experiments:
            self.create_serial_expt(num_nodes, rpn, jpn)
            num_jobs = num_nodes * jpn
            success = util.run_launcher_until(lambda: done_query.count() == num_jobs)
            self.assertEqual(done_query.count(), num_jobs)

            process_job_times()
