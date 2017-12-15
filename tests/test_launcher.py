import random
import tempfile

from tests.BalsamTestCase import BalsamTestCase, cmdline, create_job
from balsam.launcher import jobreader
from balsam.launcher.launcher import get_args
from balsam.service.models import BalsamJob
from django.conf import settings
BALSAM_SITE = settings.BALSAM_SITE

class JobReaderTests(BalsamTestCase):
    def setUp(self):
        '''several jobs, each belongs to one of 3 WFs'''
        self.NUM_JOBS = 128
        self.workflows = ['one', 'two', 'three']

        sites = f"siteA siteB {BALSAM_SITE} siteD"
        for i in range(self.NUM_JOBS):
            wf = random.choice(self.workflows)
            create_job(name=f"job{i}", site=sites, workflow=wf)

    def test_consume_all_reader(self):
        '''consume-all job reader should retreive all'''
        self.assertEqual(self.NUM_JOBS, BalsamJob.objects.count())
        config = get_args('--consume-all'.split())
        source = jobreader.JobReader.from_config(config)
        self.assertIsInstance(source, jobreader.WFJobReader)
        self.assertFalse(source.wf_name)

        source.refresh_from_db()
        self.assertEqual(len(source.jobs), self.NUM_JOBS)
        
    def test_consume_by_workflow(self):
        '''wf-name job reader should retreive only that WF'''
        config = get_args('--wf-name two'.split())
        source = jobreader.JobReader.from_config(config)
        self.assertIsInstance(source, jobreader.WFJobReader)
        self.assertTrue(source.wf_name)

        source.refresh_from_db()
        jobs_in_two = BalsamJob.objects.filter(workflow="two")
        self.assertEqual(len(source.jobs), jobs_in_two.count())
        
        source_set = set(job.pk for job in source.jobs)
        jobs_in_two_set = set(job.pk for job in jobs_in_two)
        self.assertEqual(source_set, jobs_in_two_set)
    
    def test_consume_from_file(self):
        '''job-file reader should retreive only PKs in file'''
        pks_in_file = []
        jobs = BalsamJob.objects.all()
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as jobsfile:
            fname = jobsfile.name
            for i in range(12):
                job = random.choice(jobs)
                jobsfile.write(f"{job.pk}\n")
                pks_in_file.append(job.pk)

        config = get_args(f'--job-file {fname}'.split())
        source = jobreader.JobReader.from_config(config)
        self.assertIsInstance(source, jobreader.FileJobReader)

        source.refresh_from_db()
        self.assertEqual(len(source.jobs), len(set(pks_in_file)))
        
        source_set = set(job.pk for job in source.jobs)
        self.assertEqual(source_set, set(pks_in_file))
