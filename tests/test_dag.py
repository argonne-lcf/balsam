import sys

from tests.BalsamTestCase import BalsamTestCase, cmdline
from balsam.models import BalsamJob

class BalsamDAGTests(BalsamTestCase):
    
    def setUp(self):
        '''Mock user postprocess script'''
        from importlib.util import find_spec
        self.user_script = find_spec("tests.mock_postprocess").origin

    def mock_postprocessor_run(self, job, keyword):
        '''Run the mock postprocesser as if it were happening in a Balsam Transition'''
        envs = job.get_envs()
        stdout = cmdline(' '.join([sys.executable, self.user_script, keyword]),
                      envs=envs)
        return stdout

    def test_dynamically_spawn_child(self):
        '''Can dynamically spawn a child job'''
        # One job in the Balsam DB is ready for postprocessing
        job = BalsamJob()
        job.update_state('RUN_DONE')
        self.assertEquals(BalsamJob.objects.all().count(), 1)

        # The user wrote a postprocess script using the balsamlauncher.dag API
        # Balsam transitions.py invokes the script with job-specific envs
        # The script, in turn, dynamically spawns a child process:
        self.mock_postprocessor_run(job, "spawn")

        # Now that the postprocess has run, the user confirms that there are two
        # jobs in the Balsam DB, and the second job is a child of the first
        jobs = BalsamJob.objects.all()
        self.assertEquals(jobs.count(), 2)
        parent = BalsamJob.objects.get(pk=job.pk)
        child = jobs.exclude(pk=parent.pk).first()
        self.assertIn(child, parent.get_children())

    def test_dynamically_create_job(self):
        '''Can dynamically add some jobs and dependencies'''
        # One job in the Balsam DB is ready for postprocessing
        job = BalsamJob()
        job.name = "original"
        job.update_state('RUN_DONE')
        self.assertEquals(BalsamJob.objects.all().count(), 1)

        # user postprocess script: use dag API to create 3 jobs
        # (see function mock_addjobs in mock_postprocess.py)
        self.mock_postprocessor_run(job, "addjobs")
        
        # Now there are 4 jobs
        self.assertEquals(BalsamJob.objects.all().count(), 4)
        newjobs = BalsamJob.objects.filter(name__contains="added")
        self.assertEquals(newjobs.count(), 3)

        # Job 3 depends on the completion of Job 2
        newjob1 = BalsamJob.objects.get(name="added1")
        newjob2 = BalsamJob.objects.get(name="added2")
        newjob3 = BalsamJob.objects.get(name="added3")

        self.assertFalse(newjob1.get_parents().exists())
        self.assertFalse(newjob2.get_parents().exists())
        
        parents_of_3 = newjob3.get_parents()
        self.assertEqual(parents_of_3.count(), 1)
        self.assertEqual([newjob2.pk], list(p.pk for p in parents_of_3))
    
    def test_kill_subtree(self):
        '''Can kill a subtree'''
        # Five jobs in DB: two subtrees
        # A --> B, C (children)
        # D --> E
        A = BalsamJob()
        A.name = 'A'
        A.save()
        B = BalsamJob()
        B.name = 'B'
        B.save()
        C = BalsamJob()
        C.name = 'C'
        C.save()
        D = BalsamJob()
        D.name = 'D'
        D.save()
        E = BalsamJob()
        E.name = 'E'
        E.save()
        B.set_parents([A])
        C.set_parents([A])
        E.set_parents([D])
        self.assertEquals(BalsamJob.objects.all().count(), 5)

        # user postprocess script: use dag API to kill the "A" subtree
        out = self.mock_postprocessor_run(A, "kill")
        print(out)
        
        # There are still 5 jobs
        # But now A,B,C are killed; D,E unaffected
        self.assertEquals(BalsamJob.objects.all().count(), 5)
        A = BalsamJob.objects.get(name="A")
        B = BalsamJob.objects.get(name="B")
        C = BalsamJob.objects.get(name="C")
        D = BalsamJob.objects.get(name="D")
        E = BalsamJob.objects.get(name="E")
        self.assertEquals(A.state, "USER_KILLED")
        self.assertEquals(B.state, "USER_KILLED")
        self.assertEquals(C.state, "USER_KILLED")
        self.assertEquals(D.state, "CREATED")
        self.assertEquals(E.state, "CREATED")
