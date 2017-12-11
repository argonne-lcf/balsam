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

def run_launcher_until_state(job, state):
    def check():
        job.refresh_from_db()
        return job.state == state
    success = run_launcher_until(check)
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

    def test_normal(self):
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
        success = run_launcher_until_state(job, 'JOB_FINISHED')
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
    
    def test_error_unhandled(self):
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
        success = run_launcher_until_state(job, 'FAILED')
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
    
    def test_error_handled(self):
        '''test postprocessor-handled nonzero return code'''
        
        remote_dir = tempfile.TemporaryDirectory(prefix="remote")
        remote_path = os.path.join(remote_dir.name, 'side0.dat')
        with open(remote_path, 'w') as fp:
            fp.write('9\n')

        # Same as previous test, but square.py returns nonzero
        job = create_job(name='square_testjob2', app='square',
                         args='side0.dat --retcode 1',
                         url_in=f'local:{remote_dir.name}', stage_out_files='square*',
                         url_out=f'local:{remote_dir.name}',
                         post_error_handler=True)
        self.assertEqual(job.application_args, 'side0.dat --retcode 1')
        self.assertEqual(BalsamJob.objects.all().count(), 1)
        
        # The job finished successfully despite a nonzero return code
        success = run_launcher_until_state(job, 'JOB_FINISHED')
        self.assertTrue(success)

        # Make sure at some point, it was marked with RUN_ERROR
        self.assertIn('RUN_ERROR', job.state_history)
        
        # It was saved by the postprocessor:
        self.assertIn('handled error in square_post', job.state_history)

        # We can also check the postprocessor stdout:
        work_dir = job.working_directory
        post_out = os.path.join(work_dir, 'postprocess.log')
        post_contents = open(post_out).read()
        self.assertIn("recognized error", post_contents)
        self.assertIn("Invoked to handle RUN_ERROR", post_contents)

        # job id sanity check
        jobid_line = [l for l in post_contents.split('\n') if 'jobid' in l][0]
        self.assertIn(str(job.pk), jobid_line)
    
    def test_timeout_auto_retry(self):
        '''test auto retry mechanism for timed out jobs'''
        
        remote_dir = tempfile.TemporaryDirectory(prefix="remote")
        remote_path = os.path.join(remote_dir.name, 'side0.dat')
        with open(remote_path, 'w') as fp:
            fp.write('9\n')

        # Same as previous test, but square.py hangs for 300 sec
        job = create_job(name='square_testjob2', app='square',
                         args='side0.dat --sleep 5',
                         url_in=f'local:{remote_dir.name}', stage_out_files='square*',
                         url_out=f'local:{remote_dir.name}')
                         
        # Job reaches the RUNNING state and then times out
        success = run_launcher_until_state(job, 'RUNNING')
        self.assertTrue(success)

        # On termination, actively running job is marked RUN_TIMEOUT
        def check():
            job.refresh_from_db()
            return job.state == 'RUN_TIMEOUT'

        success = poll_until_returns_true(check,timeout=6)
        self.assertTrue(success)
        self.assertEquals(job.state, 'RUN_TIMEOUT')
        
        # If we run the launcher again, it will pick up the timed out job
        success = run_launcher_until_state(job, 'JOB_FINISHED')
        self.assertTrue(success)
        self.assertIn('RESTART_READY', job.state_history)
        
        # The postprocessor was not invoked by timeout handler
        work_dir = job.working_directory
        post_out = os.path.join(work_dir, 'postprocess.log')
        post_contents = open(post_out).read()
        self.assertNotIn('handling RUN_TIMEOUT', post_contents)
    
    def test_timeout_post_handler(self):
        '''test postprocess handling option for timed-out jobs'''
        
        remote_dir = tempfile.TemporaryDirectory(prefix="remote")
        remote_path = os.path.join(remote_dir.name, 'side0.dat')
        with open(remote_path, 'w') as fp:
            fp.write('9\n')

        # Same as previous test, but square.py hangs for 300 sec
        job = create_job(name='square_testjob2', app='square',
                         args='side0.dat --sleep 5',
                         url_in=f'local:{remote_dir.name}', stage_out_files='square*',
                         url_out=f'local:{remote_dir.name}',
                         post_timeout_handler=True)
                         
        # Job reaches the RUNNING state and then times out
        success = run_launcher_until_state(job, 'RUNNING')
        self.assertTrue(success)

        # On termination, actively running job is marked RUN_TIMEOUT
        def check():
            job.refresh_from_db()
            return job.state == 'RUN_TIMEOUT'

        success = poll_until_returns_true(check,timeout=6)
        self.assertTrue(success)
        self.assertEquals(job.state, 'RUN_TIMEOUT')
        
        # If we run the launcher again, it will pick up the timed out job
        success = run_launcher_until_state(job, 'JOB_FINISHED')
        self.assertTrue(success)
        self.assertNotIn('RESTART_READY', job.state_history)
        self.assertIn('handled timeout in square_post', job.state_history)
        
        # The postprocessor handled the timeout; did not restart
        work_dir = job.working_directory
        post_out = os.path.join(work_dir, 'postprocess.log')
        post_contents = open(post_out).read()
        self.assertIn('Invoked to handle RUN_TIMEOUT', post_contents)
        self.assertIn('recognized timeout', post_contents)
    
    def test_timeout_unhandled(self):
        '''with timeout handling disabled, jobs are marked FAILED'''
        
        remote_dir = tempfile.TemporaryDirectory(prefix="remote")
        remote_path = os.path.join(remote_dir.name, 'side0.dat')
        with open(remote_path, 'w') as fp:
            fp.write('9\n')

        # Same as previous test, but square.py hangs for 300 sec
        job = create_job(name='square_testjob2', app='square',
                         args='side0.dat --sleep 5',
                         url_in=f'local:{remote_dir.name}', stage_out_files='square*',
                         url_out=f'local:{remote_dir.name}',
                         post_timeout_handler=True)
                         
        # Job reaches the RUNNING state and then times out
        success = run_launcher_until_state(job, 'RUNNING')
        self.assertTrue(success)

        # On termination, actively running job is marked RUN_TIMEOUT
        def check():
            job.refresh_from_db()
            return job.state == 'RUN_TIMEOUT'

        success = poll_until_returns_true(check,timeout=6)
        self.assertTrue(success)
        self.assertEquals(job.state, 'RUN_TIMEOUT')
        
        # If we run the launcher again, it will pick up the timed out job
        # But without timeout handling, it fails
        success = run_launcher_until_state(job, 'FAILED')
        self.assertTrue(success)
