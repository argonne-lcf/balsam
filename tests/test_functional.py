from collections import defaultdict
import glob
import os
import random
import getpass
import sys
import signal
import subprocess
import tempfile
from importlib.util import find_spec

from balsam.service.models import BalsamJob
from tests.BalsamTestCase import BalsamTestCase, cmdline
from tests.BalsamTestCase import poll_until_returns_true
from tests.BalsamTestCase import create_job, create_app


def ls_procs(keywords):
    if type(keywords) == str: keywords = [keywords]

    username = getpass.getuser()
    
    searchcmd = 'ps aux | grep '
    searchcmd += ' | grep '.join(f'"{k}"' for k in keywords) 
    grep = subprocess.Popen(searchcmd, shell=True, stdout=subprocess.PIPE)
    stdout,stderr = grep.communicate()
    stdout = stdout.decode('utf-8')

    processes = [line for line in stdout.split('\n') if 'python' in line and line.split()[0]==username]
    return processes


def sig_processes(process_lines, signal):
    for line in process_lines:
        proc = int(line.split()[1])
        try: 
            os.kill(proc, signal)
        except ProcessLookupError:
            print(f"WARNING: could not find:\n{line}\ntried to send {signal}")


def stop_launcher_processes():
    processes = ls_procs('launcher.py --consume')
    sig_processes(processes, signal.SIGTERM)
    
    def check_processes_done():
        procs = ls_procs('launcher.py --consume')
        return len(procs) == 0

    poll_until_returns_true(check_processes_done, period=2, timeout=12)
    processes = ls_procs('launcher.py --consume')
    if processes:
        print("Warning: these did not properly shutdown on SIGTERM:")
        print("\n".join(processes))
        print("Sending SIGKILL")
        sig_processes(processes, signal.SIGKILL)


def run_launcher_until(function, args=(), period=1.0, timeout=60.0):
    launcher_proc = subprocess.Popen(['balsam', 'launcher', '--consume',
                                      '--max-ranks-per-node', '8'],
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT,
                                     )
    success = poll_until_returns_true(function, args=args, period=period, timeout=timeout)
    stop_launcher_processes()
    return success

def run_launcher_seconds(seconds):
    minutes = seconds / 60.0
    launcher_path = sys.executable + " " + find_spec("balsam.launcher.launcher").origin
    launcher_path += " --consume --max-ranks 8 --time-limit-minutes " + str(minutes)
    launcher_proc = subprocess.Popen(launcher_path.split(),
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT,
                                     )
    try: launcher_proc.communicate(timeout=seconds+30)
    finally: stop_launcher_processes()


def run_launcher_until_state(job, state, period=1.0, timeout=60.0):
    def check():
        job.refresh_from_db()
        return job.state == state
    success = run_launcher_until(check, period=period, timeout=timeout)
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

        # job staged in this remote side0.dat file; it contains "9"
        staged_in_file_contents = job.read_file_in_workdir('side0.dat')
        self.assertIn('9\n', staged_in_file_contents)

        # Preprocess script actually ran:
        preproc_out_contents = job.read_file_in_workdir('preprocess.log')

        # Preprocess inherited the correct job from the environment:
        jobid_line = [l for l in preproc_out_contents.split('\n') if 'jobid' in l][0]
        self.assertIn(str(job.pk), jobid_line)

        # Preprocess recgonized the side0.dat file
        # And it altered the job application_args accordingly:
        self.assertIn('set square.py input to side0.dat', preproc_out_contents)
        self.assertIn('side0.dat', job.application_args)

        # application stdout was written to the job's .out file
        app_stdout = job.read_file_in_workdir('square_testjob.out')
        self.assertIn("Hello from square", app_stdout)

        # the square.py app wrote its result to square.dat
        app_outfile = job.read_file_in_workdir('square.dat')
        
        # The result of squaring 9 is 81
        result = float(app_outfile)
        self.assertEqual(result, 81.0)

        # the job finished normally, so square_post.py just said hello
        post_contents = job.read_file_in_workdir('postprocess.log')

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
        result = float(job.read_file_in_workdir('square.dat'))
        self.assertEqual(result, 81.0)
        
        preproc_out_contents = job.read_file_in_workdir('preprocess.log')

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
        post_contents = job.read_file_in_workdir('postprocess.log')
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

        # Same as previous test, but square.py hangs for 10 sec
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

        success = poll_until_returns_true(check,timeout=12)
        self.assertTrue(success)
        self.assertEquals(job.state, 'RUN_TIMEOUT')
        
        # If we run the launcher again, it will pick up the timed out job
        success = run_launcher_until_state(job, 'JOB_FINISHED')
        self.assertTrue(success)
        self.assertIn('RESTART_READY', job.state_history)
        
        # The postprocessor was not invoked by timeout handler
        post_contents = job.read_file_in_workdir('postprocess.log')
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

        success = poll_until_returns_true(check,timeout=12)
        self.assertEquals(job.state, 'RUN_TIMEOUT')
        
        # If we run the launcher again, it will pick up the timed out job
        success = run_launcher_until_state(job, 'JOB_FINISHED')
        self.assertTrue(success)
        self.assertNotIn('RESTART_READY', job.state_history)
        self.assertIn('handled timeout in square_post', job.state_history)
        
        # The postprocessor handled the timeout; did not restart
        post_contents = job.read_file_in_workdir('postprocess.log')
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
                         auto_timeout_retry=False)
                         
        # Job reaches the RUNNING state and then times out
        success = run_launcher_until_state(job, 'RUNNING')
        self.assertTrue(success)

        # On termination, actively running job is marked RUN_TIMEOUT
        def check():
            job.refresh_from_db()
            return job.state == 'RUN_TIMEOUT'

        success = poll_until_returns_true(check,timeout=12)
        self.assertEqual(job.state, 'RUN_TIMEOUT')
        
        # If we run the launcher again, it will pick up the timed out job
        # But without timeout handling, it fails
        success = run_launcher_until_state(job, 'FAILED')
        self.assertTrue(success)

class TestDAG(BalsamTestCase):
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

    def test_dag_error_timeout_mixture(self):
        '''test error/timeout handling mechanisms on 81 jobs (takes a couple min)'''

        # We will run 3*3*3 triples of jobs (81 total)
        # Each triple is a tree with 1 parent and 2 children
        # Try every possible permutation of normal/timeout/fail
        # Timeout jobs sleep for a couple seconds and have a higher chance of
        # being interrupted and timed-out; this is not guaranteed though
        from itertools import product
        states = 'normal timeout fail'.split()
        triplets = product(states, repeat=3)

        # Parent job template
        parent_types = {
            'normal': create_job(name='make_sides', app='make_sides',
                                 args='',
                                 post_error_handler=True,
                                 post_timeout_handler=True,
                                 wtime=0),
            'timeout': create_job(name='make_sides', app='make_sides',
                                 args='--sleep 2',
                                  post_error_handler=True,
                                  post_timeout_handler=True,
                                  wtime=0),
            'fail': create_job(name='make_sides', app='make_sides',
                               args='--retcode 1',
                               post_error_handler=True,
                               post_timeout_handler=True,
                               wtime=0),
        }
        
        # Child job template
        child_types = {
            'normal': create_job(name='square', app='square', args='',
                                 post_error_handler=True,
                                 post_timeout_handler=True,
                                 wtime=0),
            'timeout': create_job(name='square', app='square', args='--sleep 2',
                                  post_error_handler=True,
                                  post_timeout_handler=True,
                                  wtime=0),
            'fail': create_job(name='square', app='square', args='--retcode 1',
                               post_error_handler=True,
                               post_timeout_handler=True,
                               wtime=0),
        }

        # Create all 81 jobs
        job_triplets = {}
        for triplet in triplets:
            parent, childA, childB = triplet

            # Load the template
            jobP = BalsamJob.objects.get(pk=parent_types[parent].pk)
            jobA = BalsamJob.objects.get(pk=child_types[childA].pk)
            jobB = BalsamJob.objects.get(pk=child_types[childB].pk)
            jobP.pk, jobA.pk, jobB.pk = None,None,None
            for job in (jobP,jobA,jobB):
                job.working_directory = ''
                job.save()
            
            # Parent has two children (sides); either 1 rank (serial) or 2 ranks (mpi)
            NUM_SIDES, NUM_RANKS = 2, random.randint(1,2)
            pre = self.apps['make_sides'].default_preprocess + f' {NUM_SIDES} {NUM_RANKS}'
            jobP.preprocess = pre
            jobP.save()

            jobA.application_args += "  side0.dat"
            jobA.input_files += "side0.dat"
            jobA.save()
            jobA.set_parents([jobP])

            jobB.application_args += "  side1.dat"
            jobB.input_files += "side1.dat"
            jobB.save()
            jobB.set_parents([jobP])

            job_triplets[triplet] = (jobP, jobA, jobB)

        # Remove jobs that were only used as template
        for j in parent_types.values(): j.delete()
        for j in child_types.values(): j.delete()
        del parent_types, child_types
        self.assertEqual(BalsamJob.objects.all().count(), 81)
        
        for job in BalsamJob.objects.all():
            self.assertEqual(job.working_directory, '')

        # Run the entire DAG until finished, with two interruptions

        def check(N_run, N_finish):
            running = BalsamJob.objects.filter(state='RUNNING')
            finished = BalsamJob.objects.filter(state='JOB_FINISHED')
            return running.count() >= N_run and finished.count() >= N_finish
        run_launcher_until(check, args=(1, 1)) # interrupt at least 1
        run_launcher_until(check, args=(2, 5)) # interrupt at least 2

        # Get rid of the sleep now to speed up finish
        slow_jobs = BalsamJob.objects.filter(application_args__contains="sleep")
        for job in slow_jobs:
            job.application_args = '--sleep 0'
            job.save()
        
        def check():
            return all(j.state == 'JOB_FINISHED' for j in BalsamJob.objects.all())

        # Just check that all jobs reach JOB_FINISHED state
        success = run_launcher_until(check, timeout=360.0)
        self.assertTrue(success)

        # No race conditions in working directory naming: each job must have a
        # unique working directory
        workdirs = [job.working_directory for job in BalsamJob.objects.all()]
        self.assertEqual(len(workdirs), len(set(workdirs)))

    def test_static(self):
        '''test normal processing of a pre-defined DAG'''

        NUM_SIDES, NUM_RANKS = 3, 2
        pre = self.apps['make_sides'].default_preprocess + f' {NUM_SIDES} {NUM_RANKS}'
        parent = create_job(name='make_sides', app='make_sides', 
                            preproc=pre)
        
        # Each side length is mapped to a square area in a set of mapping jobs.
        # These 3 "square_jobs" all have the same parent make_sides, but each
        # takes a different input file
        square_jobs = {
            i : create_job(
            name=f'square{i}', app='square',
            args=f'side{i}.dat', input_files=f'side{i}.dat'
            )
            for i in range(NUM_SIDES)
        }
        for job in square_jobs.values():
            job.set_parents([parent])

        # The final reduce job depends on all the square jobs: all square.dat
        # files will be staged in and final results staged out to a remote
        # directory
        remote_dir = tempfile.TemporaryDirectory(prefix="remote")
        reduce_job = create_job(name='reduce', app='reduce',
                                input_files="square*.dat*",
                                url_out=f'local:{remote_dir.name}',
                                stage_out_files='summary*.dat reduce.out'
                                )
        reduce_job.set_parents(square_jobs.values())
        
        # Run the entire DAG until finished
        success = run_launcher_until_state(reduce_job, 'JOB_FINISHED',
                                           timeout=180.0)
        self.assertTrue(success)
        for job in (parent, *square_jobs.values(), reduce_job):
            job.refresh_from_db()
            self.assertEqual(job.state, 'JOB_FINISHED')

        # Double-check the calculation result; thereby testing flow of data
        workdir = parent.working_directory
        files = (os.path.join(workdir, f"side{i}.dat") for i in range(NUM_SIDES))
        sides = [float(open(f).read()) for f in files]
        self.assertTrue(all(0.5 <= s <= 5.0 for s in sides))
        expected_result = sum(s**2 for s in sides)

        resultpath = os.path.join(remote_dir.name, 'reduce.out')
        result = open(resultpath).read()
        self.assertIn('Total area:', result)
        result = float(result.split()[-1])
        self.assertAlmostEqual(result, expected_result)

    def triplet_data_check(self, parent, A, B):
        '''helper function for the below tests'''
        side0 = float(parent.read_file_in_workdir('side0.dat'))
        side1 = float(parent.read_file_in_workdir('side1.dat'))
        square0 = float(A.read_file_in_workdir('square.dat'))
        square1 = float(B.read_file_in_workdir('square.dat'))

        self.assertAlmostEqual(side0**2, square0)
        self.assertAlmostEqual(side1**2, square1)

    def test_child_timeout(self):
        '''timeout handling in a dag'''

        # Same DAG triplet as above: one parent with 2 children A & B
        NUM_SIDES, NUM_RANKS = 2, 1
        pre = self.apps['make_sides'].default_preprocess + f' {NUM_SIDES} {NUM_RANKS}'
        parent = create_job(name='make_sides', app='make_sides', preproc=pre)
        
        chA = create_job(name='square0', app='square', args='side0.dat',
                         input_files='side0.dat')
        chB = create_job(name='square1', app='square', args='side1.dat --sleep 30',
                         input_files='side1.dat', post_timeout_handler=True)
        chA.set_parents([parent])
        chB.set_parents([parent])

        # Run until A finishes, but B will still be hanging
        def check():
            chA.refresh_from_db()
            chB.refresh_from_db()
            return chA.state=='JOB_FINISHED' and chB.state=='RUNNING'

        success = run_launcher_until(check)
        self.assertTrue(success)

        # Give the launcher time to clean up and mark B as timed out
        def check():
            chB.refresh_from_db()
            return chB.state == 'RUN_TIMEOUT'
        success = poll_until_returns_true(check, timeout=12)
        self.assertEqual(chB.state, 'RUN_TIMEOUT')

        # Since B has a timeout handler, when we re-run the launcher, 
        # It is handled gracefully
        success = run_launcher_until_state(chB, 'JOB_FINISHED')

        parent.refresh_from_db()
        chA.refresh_from_db()
        self.assertEqual(parent.state, 'JOB_FINISHED')
        self.assertEqual(chA.state, 'JOB_FINISHED')
        self.assertEqual(chB.state, 'JOB_FINISHED')

        # The data-flow was correct
        self.triplet_data_check(parent, chA, chB)
        
        # The post-processor in fact handled the timeout
        self.assertIn('recognized timeout',
                      chB.read_file_in_workdir('postprocess.log'))
    
    def test_child_error(self):
        '''error handling in a dag'''
        
        # Same DAG triplet as above: one parent with 2 children A & B
        NUM_SIDES, NUM_RANKS = 2, 1
        pre = self.apps['make_sides'].default_preprocess + f' {NUM_SIDES} {NUM_RANKS}'
        parent = create_job(name='make_sides', app='make_sides', preproc=pre)
        
        chA = create_job(name='square0', app='square', args='side0.dat',
                         input_files='side0.dat')
        chB = create_job(name='square1', app='square', args='side1.dat --retcode 1',
                         input_files='side1.dat', post_error_handler=True)
        chA.set_parents([parent])
        chB.set_parents([parent])

        # child B will give a RUN_ERROR, but it will be handled
        def check():
            return all(j.state=='JOB_FINISHED' for j in BalsamJob.objects.all())

        success = run_launcher_until(check, timeout=120)
        self.assertTrue(success)
        
        parent.refresh_from_db()
        chA.refresh_from_db()
        chB.refresh_from_db()

        self.assertEqual(parent.state, 'JOB_FINISHED')
        self.assertEqual(chA.state, 'JOB_FINISHED')
        self.assertEqual(chB.state, 'JOB_FINISHED')

        # Data flow was correct
        self.triplet_data_check(parent, chA, chB)

        # The post-processor handled the nonzero return code in B
        self.assertIn('recognized error',
                      chB.read_file_in_workdir('postprocess.log'))
    
    def test_parent_timeout(self):
        '''timeout handling (with rescue job) in a dag'''
        
        # Same DAG triplet as above: one parent with 2 children A & B
        NUM_SIDES, NUM_RANKS = 2, 1
        pre = self.apps['make_sides'].default_preprocess + f' {NUM_SIDES} {NUM_RANKS}'
        parent = create_job(name='make_sides', app='make_sides', preproc=pre,
                            args='--sleep 30',post_timeout_handler=True)
        
        chA = create_job(name='square0', app='square', args='side0.dat',
                         input_files='side0.dat')
        chB = create_job(name='square1', app='square', args='side1.dat',
                         input_files='side1.dat')
        chA.set_parents([parent])
        chB.set_parents([parent])

        # We run the launcher and kill it once parent starts running
        success = run_launcher_until_state(parent, 'RUNNING')
        self.assertTrue(success)
        
        # Parent timed out
        def check():
            parent.refresh_from_db()
            return parent.state == 'RUN_TIMEOUT'
        success = poll_until_returns_true(check,timeout=12)
        self.assertTrue(success)

        parent.refresh_from_db()
        chA.refresh_from_db()
        chB.refresh_from_db()
        self.assertEqual(parent.state, 'RUN_TIMEOUT')
        self.assertEqual(chA.state, 'AWAITING_PARENTS')
        self.assertEqual(chB.state, 'AWAITING_PARENTS')
        
        # On re-run, everything finishes okay
        def check():
            chA.refresh_from_db()
            chB.refresh_from_db()
            return chA.state=='JOB_FINISHED' and chB.state=='JOB_FINISHED'
        success = run_launcher_until(check, timeout=120)

        parent.refresh_from_db()
        chA.refresh_from_db()
        chB.refresh_from_db()

        self.assertEqual(parent.state, 'JOB_FINISHED')
        self.assertEqual(chA.state, 'JOB_FINISHED')
        self.assertEqual(chB.state, 'JOB_FINISHED')

        
        # What happened: a rescue job was created by the time-out handler and
        # ran in the second launcher invocation
        jobs = BalsamJob.objects.all()
        self.assertEqual(jobs.count(), 4)

        # This rescue job was made to be the parent of A and B
        rescue_job = chB.get_parents().first()
        self.assertEqual(rescue_job.state, 'JOB_FINISHED')

        # The job state history shows how this happened:
        self.assertIn(f'spawned by {parent.cute_id}', rescue_job.state_history)
        self.assertIn(f'spawned rescue job {rescue_job.cute_id}', parent.state_history)
        
        # It happened during the post-processing step:
        post_log = parent.read_file_in_workdir('postprocess.log')
        self.assertIn('Creating rescue job', post_log)
        
        # Data flow correct:
        self.triplet_data_check(rescue_job, chA, chB)
        
    
    def test_parent_error(self):
        '''test dag error handling'''
        
        # Same DAG triplet as above: one parent with 2 children A & B
        NUM_SIDES, NUM_RANKS = 2, 1
        pre = self.apps['make_sides'].default_preprocess + f' {NUM_SIDES} {NUM_RANKS}'
        parent = create_job(name='make_sides', app='make_sides', preproc=pre,
                            args='--retcode 1',post_error_handler=True)
        
        chA = create_job(name='square0', app='square', args='side0.dat',
                         input_files='side0.dat')
        chB = create_job(name='square1', app='square', args='side1.dat',
                         input_files='side1.dat')
        chA.set_parents([parent])
        chB.set_parents([parent])

        # Parent will give an error, but it will be handled
        def check():
            parent.refresh_from_db()
            chA.refresh_from_db()
            chB.refresh_from_db()
            jobs = parent,chA,chB
            return all(j.state == 'JOB_FINISHED' for j in jobs)

        # Everything finished successfully
        success = run_launcher_until(check, timeout=120)
        self.assertTrue(success)

        parent.refresh_from_db()
        chA.refresh_from_db()
        chB.refresh_from_db()
        
        # The parent state history shows that an error was handled
        self.assertIn(f'RUN_ERROR', parent.state_history)
        self.assertIn(f'handled error; it was okay', parent.state_history)
        
        # The post-processor handled it
        post_log = parent.read_file_in_workdir('postprocess.log')
        self.assertIn('the job was actually done', post_log)
        
        # Data flow okay:
        self.triplet_data_check(parent, chA, chB)
        
        # no rescue jobs had to be created:
        jobs = BalsamJob.objects.all()
        self.assertEqual(jobs.count(), 3)
    
    def test_dynamic(self):
        '''test dynamic generation of child jobs'''

        # The parent will create between 4 and 8 child jobs in the course
        # of its post-processing step:
        NUM_SIDES, NUM_RANKS = random.randint(4,8), 1
        pre = self.apps['make_sides'].default_preprocess + f' {NUM_SIDES} {NUM_RANKS}'
        post = self.apps['make_sides'].default_postprocess + ' --dynamic-spawn'
        parent = create_job(name='make_sides', app='make_sides', 
                            preproc=pre, postproc=post)
        
        # The final reduce job will depend on all these spawned child jobs, but
        # they do not exist yet!  We will allow these dependencies to be
        # established dynamically; for now the reduce step just depends on the
        # top-level parent of the tree.
        remote_dir = tempfile.TemporaryDirectory(prefix="remote")
        reduce_job = create_job(name='sum_squares', app='reduce',
                                input_files="square*.dat*",
                                url_out=f'local:{remote_dir.name}',
                                stage_out_files='summary?.dat *.out'
                                )
        reduce_job.set_parents([parent])
        
        # Run the entire DAG until finished
        success = run_launcher_until_state(reduce_job, 'JOB_FINISHED',
                                           timeout=200.0)
        self.assertTrue(success)
        for job in BalsamJob.objects.all():
            self.assertEqual(job.state, 'JOB_FINISHED')

        # Double-check the calculation result; thereby testing flow of data
        workdir = parent.working_directory
        files = (os.path.join(workdir, f"side{i}.dat") for i in range(NUM_SIDES))
        sides = [float(open(f).read()) for f in files]
        self.assertTrue(all(0.5 <= s <= 5.0 for s in sides))
        expected_result = sum(s**2 for s in sides)

        resultpath = os.path.join(remote_dir.name, 'sum_squares.out')
        result = open(resultpath).read()
        self.assertIn('Total area:', result)
        result = float(result.split()[-1])
        self.assertAlmostEqual(result, expected_result)

        # Checking the post-processor log, we see that those jobs were actually
        # spawned. 
        post_contents = parent.read_file_in_workdir('postprocess.log')
        for i in range(NUM_SIDES):
            self.assertIn(f'spawned square{i} job', post_contents)

        # The spawned jobs' state histories confirm this.
        square_jobs = BalsamJob.objects.filter(name__startswith='square')
        self.assertEqual(square_jobs.count(), NUM_SIDES)
        for job in square_jobs:
            self.assertIn(f'spawned by {parent.cute_id}', job.state_history)

        # Make sure that the correct number of dependencies were created for the
        # reduce job: one for each dynamically-spawned job (plus the original)
        self.assertEqual(reduce_job.get_parents().count(), NUM_SIDES+1)

class TestThreadPlacement(BalsamTestCase):

    def setUp(self):
        self.app_path = os.path.dirname(find_spec("tests.c_apps").origin)
        self.app = create_app(name='omp')

        self.job0 = create_job(name='job0', app='omp', num_nodes=2, ranks_per_node=32, threads_per_rank=2)
        self.job1 = create_job(name='job1', app='omp', num_nodes=2, ranks_per_node=64, threads_per_rank=1)
        self.job2 = create_job(name='job2', app='omp', num_nodes=1, ranks_per_node=2, threads_per_rank=64, threads_per_core=2)

    def check_omp_exe_output(self, job):
        fname = job.name + '.out'
        out = job.read_file_in_workdir(fname)
        
        proc_counts = defaultdict(int)
        ranks = []
        threads = []

        for line in out.split('\n'):
            dat = line.split()
            if len(dat) == 3:
                name, rank, thread = dat
                proc_counts[name] += 1
                ranks.append(int(rank))
                threads.append(int(thread))

        proc_names = proc_counts.keys()
        self.assertEqual(len(proc_names), job.num_nodes)
        if job.num_nodes == 2:
            proc1, proc2 = proc_names
            self.assertEqual(proc_counts[proc1], proc_counts[proc2])

        expected_ranks = list(range(job.num_ranks)) * job.threads_per_rank
        self.assertListEqual(sorted(ranks), sorted(expected_ranks))

        expected_threads = list(range(job.threads_per_rank)) * job.num_ranks
        self.assertListEqual(sorted(threads), sorted(expected_threads))

    def test_Theta(self):
        '''MPI/OMP C binary for Theta: check thread/rank placement'''
        from balsam.service.schedulers import Scheduler
        scheduler = Scheduler.scheduler_main
        if scheduler.host_type != 'CRAY':
            self.skipTest('did not recognize Cray environment')

        if scheduler.num_workers < 2:
            self.skipTest('need at least two nodes reserved to run this test')
        
        binary = glob.glob(os.path.join(self.app_path, 'omp.theta.x'))
        self.app.executable = binary[0]
        self.app.save()

        def check():
            jobs = BalsamJob.objects.all()
            return all(j.state == 'JOB_FINISHED' for j in jobs)

        run_launcher_until(check)
        self.job0.refresh_from_db()
        self.job1.refresh_from_db()
        self.job2.refresh_from_db()

        self.assertEqual(self.job0.state, 'JOB_FINISHED')
        self.assertEqual(self.job1.state, 'JOB_FINISHED')
        self.assertEqual(self.job2.state, 'JOB_FINISHED')

        # Check output of dummy MPI/OpenMP C program
        self.check_omp_exe_output(self.job0)
        self.check_omp_exe_output(self.job1)
        self.check_omp_exe_output(self.job2)
