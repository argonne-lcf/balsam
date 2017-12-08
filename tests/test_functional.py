import os
import random
import sys
import signal
import time
import subprocess
import tempfile
from importlib.util import find_spec

from balsam.models import BalsamJob
from tests.BalsamTestCase import BalsamTestCase, cmdline
from tests.BalsamTestCase import poll_until_returns_true
from tests.BalsamTestCase import create_job, create_app


BALSAM_TEST_DIR = os.environ['BALSAM_TEST_DIRECTORY']

def run_launcher_until(function):
    launcher_proc = subprocess.Popen(['balsam', 'launcher', '--consume'],
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT,
                                     preexec_fn=os.setsid)
    success = poll_until_returns_true(function, timeout=20)

    # WEIRDEST BUG IN TESTING IF YOU OMIT THE FOLLOIWNG STATEMENT!
    # launcher_proc.terminate() doesn't work; the process keeps on running and
    # then you have two launchers from different test cases processing the same
    # job...Very hard to catch bug.
    os.killpg(os.getpgid(launcher_proc.pid), signal.SIGTERM)  # Send the signal to all the process groups
    launcher_proc.wait(timeout=10)
    return success


class TestSingleJobTransitions(BalsamTestCase):
    def setUp(self):
        aliases = "make_sides square reduce".split()
        self.apps = {}
        for name in aliases:
            interpreter = sys.executable
            exe_path = interpreter + " " + find_spec(f'tests.ft_apps.{name}').origin
            pre_path = interpreter + " " + find_spec(f'tests.ft_apps.{name}_pre').origin
            post_path = interpreter + " " + find_spec(f'tests.ft_apps.{name}_post').origin
            app = create_app(name=name, executable=exe_path, preproc=pre_path,
                             postproc=post_path)
            self.apps[name] = app

    def test_one_job_normal(self):
        '''normal processing of a single job'''
        
        # A mock "remote" data source has a file side0.dat
        # This file contains the side length of a square: 9
        remote_dir = tempfile.TemporaryDirectory(prefix="remote")
        remote_path = os.path.join(remote_dir.name, 'side0.dat')
        with open(remote_path, 'w') as fp:
            fp.write('9\n')

        job = create_job(name='square_testjob', app='square',
                         url_in=f'local:{remote_dir.name}', stage_out_files='square*', 
                         url_out=f'local:{remote_dir.name}',
                         args='')

        # Sanity check test case isolation
        self.assertEquals(job.state, 'CREATED')
        self.assertEqual(job.application_args, '')
        self.assertEqual(BalsamJob.objects.all().count(), 1)
        
        # Run the launcher and make sure that the job gets carried all the way
        # through to completion
        def check():
            job.refresh_from_db()
            return job.state == 'JOB_FINISHED'
        success = run_launcher_until(check)
        self.assertTrue(success)

        work_dir = job.working_directory

        # job staged in this remote side0.dat file; it's really here now
        staged_in_file = os.path.join(work_dir, 'side0.dat')
        self.assertTrue(os.path.exists(staged_in_file))

        # And it contains "9"
        staged_in_file_contents = open(staged_in_file).read()
        self.assertIn('9\n', staged_in_file_contents)

        # Preprocess script actually ran:
        preproc_out = os.path.join(work_dir, 'preprocess.log')
        self.assertTrue(os.path.exists(preproc_out))
        preproc_out_contents = open(preproc_out).read()

        # Preprocess inherited the correct job from the environment:
        jobid_line = [l for l in preproc_out_contents.split('\n') if 'jobid' in l][0]
        self.assertIn(str(job.pk), jobid_line)

        # Preprocess recgonized the side0.dat file
        # And it altered the job application_args accordingly:
        self.assertIn('set square.py input to side0.dat', preproc_out_contents)
        self.assertIn('side0.dat', job.application_args)

        # application stdout was written to the job's .out file
        app_stdout = os.path.join(work_dir, 'square_testjob.out')
        self.assertTrue(os.path.exists(app_stdout))
        self.assertIn("Hello from square", open(app_stdout).read())

        # the square.py app wrote its result to square.dat
        app_outfile = os.path.join(work_dir, 'square.dat')
        self.assertTrue(os.path.exists(app_outfile))
        
        # The result of squaring 9 is 81
        result = float(open(app_outfile).read())
        self.assertEqual(result, 81.0)

        # the job finished normally, so square_post.py just said hello
        post_outfile = os.path.join(work_dir, 'postprocess.log')
        self.assertTrue(os.path.exists(post_outfile))
        post_contents = open(post_outfile).read()

        jobid_line = [l for l in post_contents.split('\n') if 'jobid' in l][0]
        self.assertIn(str(job.pk), jobid_line)
        self.assertIn('hello from square_post', post_contents)

        # After stage out, the remote directory contains two new files
        # That matched the pattern square*  ....
        # square.dat and square_testjob.out
        remote_square = os.path.join(remote_dir.name, 'square.dat')
        remote_stdout = os.path.join(remote_dir.name, 'square_testjob.out')
        self.assertTrue(os.path.exists(remote_square))
        self.assertTrue(os.path.exists(remote_stdout))
        
        result_remote = float(open(remote_square).read())
        self.assertEquals(result_remote, 81.0)
        self.assertIn("Hello from square", open(remote_stdout).read())
    
    def test_one_job_error_unhandled(self):
        '''test unhandled return code from app'''
        
        remote_dir = tempfile.TemporaryDirectory(prefix="remote")
        remote_path = os.path.join(remote_dir.name, 'side0.dat')
        with open(remote_path, 'w') as fp:
            fp.write('9\n')

        # Same as previous test, but square.py returns nonzero
        job = create_job(name='square_testjob2', app='square',
                         args='side0.dat --retcode 1',
                         url_in=f'local:{remote_dir.name}', stage_out_files='square*',
                         url_out=f'local:{remote_dir.name}')
        self.assertEqual(job.application_args, 'side0.dat --retcode 1')
        self.assertEqual(BalsamJob.objects.all().count(), 1)
        
        # The job is marked FAILED due to unhandled nonzero return code
        def check():
            job.refresh_from_db()
            return job.state == 'FAILED'
        success = run_launcher_until(check)
        self.assertTrue(success)
        
        # (But actually the application ran and printed its result correctly)
        work_dir = job.working_directory
        out_path = os.path.join(work_dir, 'square.dat')
        result = float(open(out_path).read())
        self.assertEqual(result, 81.0)
        
        
        preproc_out = os.path.join(work_dir, 'preprocess.log')
        self.assertTrue(os.path.exists(preproc_out))
        preproc_out_contents = open(preproc_out).read()

        jobid_line = [l for l in preproc_out_contents.split('\n') if 'jobid' in l][0]
        self.assertIn(str(job.pk), jobid_line)
