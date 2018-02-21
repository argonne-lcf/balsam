from datetime import datetime
from importlib.util import find_spec

from balsam.service.models import BalsamJob
from tests.BalsamTestCase import BalsamTestCase
from tests.BalsamTestCase import create_job, create_app

from tests import util
    
class TestNoOp(BalsamTestCase):

    def setUp(self):
        from itertools import product

        self.launcherInfo = util.launcher_info()
        max_workers = self.launcherInfo.num_workers

        num_nodes = [2**n for n in range(0,13) if 2**n <= max_workers]
        if num_nodes[-1] != max_workers:
            num_nodes.append(max_workers)

        rpn = [64]
        jpn = [64, 512]
        #jpn = [16]
        self.experiments = product(num_nodes, rpn, jpn)

    def create_serial_expt(self, num_nodes, rpn, jpn):
        '''Populate DB with set number of dummy serial jobs; no deps'''
        BalsamJob.objects.all().delete()
        num_jobs = num_nodes * jpn
        
        jobs = [create_job(name=f'task{i}', direct_command=f'echo Hello',
                         args=str(i), workflow='bench-no-op', save=False)
                         for i in range(num_jobs)]
        BalsamJob.objects.bulk_create(jobs)
        self.assertEqual(BalsamJob.objects.count(), num_jobs)

    def test_serial(self):
        '''Populate DB, run launcher, get timing data from job histories
        Serial: all jobs pack into MPIEnsembles and can run concurrently'''
        done_query = BalsamJob.objects.filter(state='JOB_FINISHED')

        for (num_nodes, rpn, jpn) in self.experiments:
            title = f'{num_nodes}nodes_{rpn}rpn_{jpn}jpn'
            self.create_serial_expt(num_nodes, rpn, jpn)
            num_jobs = num_nodes * jpn

            launcher_start_time = datetime.now()
            success = util.run_launcher_until(lambda: done_query.count() == num_jobs, 
                                              timeout=1000, maxrpn=rpn)
            self.assertEqual(done_query.count(), num_jobs)

            time_data = util.process_job_times(time0=launcher_start_time)
            self.assertEqual(len(time_data['PREPROCESSED']), num_jobs)
            self.assertEqual(len(time_data['JOB_FINISHED']), num_jobs)

            cdf_table = util.print_jobtimes_cdf(time_data)
            resultpath = util.benchmark_outfile_path('serial_no_op.dat')

            with open(resultpath, 'w') as fp:
                title = f'# {num_nodes} nodes, {rpn} rpn, {jpn} jpn ({num_jobs} total jobs)'
                comment = 'All jobs pack into MPIEnsembles and can run concurrently'
                fp.write(util.FormatTable.create_header(title, comment))
                fp.write(cdf_table)
