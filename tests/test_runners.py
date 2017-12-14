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


class TestMPIRunner(BalsamTestCase):
    '''start, update_jobs, finished, error/timeout handling'''
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

        app_path = f"{sys.executable}  {find_spec('tests.mock_mpi_app').origin}"
        self.app = create_app(name="mock_mpi",description="print and sleep",
                              executable=app_path)
        
        # Test various worker configurations:
        self.work_configs = []
        WorkerConfig = namedtuple('WorkerConfig', ['workers', 'num_nodes',
                                                   'ranks_per_node'])
        # 2 ranks on one node
        node0 = self.worker_group[0]
        cfg = WorkerConfig([node0], 1, 2)
        self.work_configs.append(cfg)
        
        # max ranks on one node
        cfg = WorkerConfig([node0], 1, node0.max_ranks_per_node)
        self.work_configs.append(cfg)
        
        # max ranks on all nodes
        cfg = WorkerConfig(list(self.worker_group), len(self.worker_group),
                           node0.max_ranks_per_node)
        self.work_configs.append(cfg)

    
    def assert_output_file_contains_n_ranks(self, fp, n):
        '''specific check of mock_mpi_app.py output'''
        found = []
        for line in fp:
            found.append(int(line.split()[1]))
        self.assertSetEqual(set(range(n)), set(found))

    def test_normal(self):
        '''MPI application runs, returns 0, marked RUN_DONE'''
        for i, (workerslist, num_nodes, rpn) in enumerate(self.work_configs):
            job = create_job(name=f"test{i}", app=self.app.name,
                             num_nodes=num_nodes, ranks_per_node=rpn)
            self.assertEquals(job.state, 'CREATED')

            runner = runners.MPIRunner([job], workerslist)

            # Start the job and update state right away
            # If it didn't finish too fast, it should now be RUNNING
            runner.start()
            runner.update_jobs()
            if not runner.finished():
                self.assertEquals(job.state, 'RUNNING')

            # Now wait for the job to finish
            # On sucessful run, it should be RUN_DONE
            poll_until_returns_true(runner.finished, period=0.5, timeout=40)
            self.assertTrue(runner.finished())
            runner.update_jobs()
            self.assertEquals(job.state, 'RUN_DONE')

            # Check that the correct output is really there:
            outpath = runner.outfile.name
            with open(outpath) as fp:
                self.assert_output_file_contains_n_ranks(fp, num_nodes*rpn)

    
    def test_return_nonzero(self):
        '''MPI application runs, return 255, marked RUN_ERROR'''
        for i, (workerslist, num_nodes, rpn) in enumerate(self.work_configs):
            job = create_job(name=f"test{i}", app=self.app.name,
                             num_nodes=num_nodes, ranks_per_node=rpn,
                             args='--retcode 255')

            self.assertEquals(job.state, 'CREATED')
            runner = runners.MPIRunner([job], workerslist)
            runner.start()
            
            poll_until_returns_true(runner.finished, period=0.5)
            runner.update_jobs()
            self.assertEquals(job.state, 'RUN_ERROR')
    
    def test_timeouts(self):
        '''MPI application runs for too long, call timeout, marked RUN_TIMEOUT'''
        for i, (workerslist, num_nodes, rpn) in enumerate(self.work_configs):
            job = create_job(name=f"test{i}", app=self.app.name,
                             num_nodes=num_nodes, ranks_per_node=rpn,
                             args='--sleep 10')

            self.assertEquals(job.state, 'CREATED')
            runner = runners.MPIRunner([job], workerslist)

            # job starts running; sleeps for 10 seconds
            runner.start()
            runner.update_jobs()
            self.assertEquals(job.state, 'RUNNING')

            # we wait just 2 seconds and the job is still going 
            time.sleep(2)
            self.assertEquals(job.state, 'RUNNING')

            # Timeout the runner
            # Now the job is marked as RUN_TIMEOUT
            runner_group = runners.RunnerGroup(Lock())
            runner_group.runners.append(runner)
            runner_group.update_and_remove_finished(timeout=True)
            self.assertEquals(job.state, 'RUN_TIMEOUT')

            # A moment later, the runner process is indeed terminated
            term = poll_until_returns_true(runner.finished, period=0.1, 
                                           timeout=6.0)
            self.assertTrue(term)
    
class TestMPIEnsemble(BalsamTestCase):
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
        self.app = create_app(name="mock_serial", description="square a number",
                              executable=app_path)

    def test_MPIEnsembleRunner(self):
        '''Several non-MPI jobs packaged into one mpi4py wrapper'''
        num_ranks = sum(w.num_nodes*w.max_ranks_per_node for w in
                        self.worker_group)
        num_jobs_per_type = num_ranks // 3

        jobs = {'qsub' : [], # these have no AppDef, will run ok
                'normal':[], # these will succeed as well
                'fail'  :[], # these should be RUN_ERROR
                'timeout':[] # these should be RUN_TIMEOUT
                }
        args = {'normal' : '',
                'fail' : '--retcode 1',
                'timeout' : '--sleep 25'
                }
        for jobtype in 'qsub normal fail'.split():
            for i in range(num_jobs_per_type):
                if jobtype == 'qsub':
                    cmd = f'echo hello world {i}'
                    app, appargs = '', ''
                else:
                    cmd = ''
                    app, appargs = self.app.name, f"{i} {args[jobtype]}"

                job = create_job(name=f"{jobtype}{i}", app=app,
                                 direct_command=cmd, args=appargs)
                jobs[jobtype].append(job)

        shuffled_jobs = [j for joblist in jobs.values() for j in joblist]
        random.shuffle(shuffled_jobs)
        
        # We want to put timeout jobs at the end of this list
        app, appargs = self.app.name, f"{i} {args['timeout']}"
        for i in range(num_jobs_per_type):
            job = create_job(name=f"timeout{i}", app=app,
                             direct_command='', args=appargs)
            jobs['timeout'].append(job)
            shuffled_jobs.append(job)
        
        all_workers = list(self.worker_group)
        runner = runners.MPIEnsembleRunner(shuffled_jobs, all_workers)

        for job in shuffled_jobs:
            self.assertEqual(job.state, 'CREATED')

        # start the ensemble
        runner.start()

        # All of the qsub, normal, and fail jobs should be done quickly
        # Let's give it up to 12 seconds, checking once a second
        def check_done():
            runner.update_jobs()
            normal_done = all(j.state=='RUN_DONE' for j in jobs['normal'])
            qsub_done   = all(j.state=='RUN_DONE' for j in jobs['qsub'])
            error_done  = all(j.state=='RUN_ERROR' for j in jobs['fail'])
            return normal_done and qsub_done and error_done

        finished = poll_until_returns_true(check_done, period=1, timeout=20)
        self.assertTrue(finished)

        # And the long-running jobs in the ensemble are still going:
        self.assertTrue(all(j.state=='RUNNING' for j in jobs['timeout']))

        # So we kill the runner. The timed-out jobs are marked accordingly
        runner_group = runners.RunnerGroup(Lock())
        runner_group.runners.append(runner)
        runner_group.update_and_remove_finished(timeout=True)

        self.assertTrue(all(j.state=='RUN_TIMEOUT' for j in jobs['timeout']))

        # Double-check that the rest of the jobs are unaffected
        self.assertTrue(all(j.state=='RUN_DONE' for j in jobs['normal']))
        self.assertTrue(all(j.state=='RUN_DONE' for j in jobs['qsub']))
        self.assertTrue(all(j.state=='RUN_ERROR' for j in jobs['fail']))

        # Kill the sleeping jobs in case they do not terminate
        killcmd = "ps aux | grep mock_serial | grep -v grep | grep -v vim | awk '{print $2}' | xargs kill -9"
        os.system(killcmd)
        killcmd = "ps aux | grep mpi_ensemble.py | grep -v grep | grep -v vim | awk '{print $2}' | xargs kill -9"
        os.system(killcmd)


class TestRunnerGroup(BalsamTestCase):
    def setUp(self):
        scheduler = Scheduler.scheduler_main
        self.host_type = scheduler.host_type
        if self.host_type == 'DEFAULT':
            config = get_args('--consume-all --num-workers 1 --max-ranks-per-node 8'.split())
        else:
            config = get_args('--consume-all'.split())

        self.worker_group = worker.WorkerGroup(config, host_type=self.host_type,
                                               workers_str=scheduler.workers_str,
                                               workers_file=scheduler.workers_file)

        app_path = f"{sys.executable}  {find_spec('tests.mock_mpi_app').origin}"
        self.mpiapp = create_app(name="mock_mpi", description="print and sleep",
                                 executable=app_path)
        
        app_path = f"{sys.executable}  {find_spec('tests.mock_serial_app').origin}"
        self.serialapp = create_app(name="mock_serial", description="square a"
                                    " number", executable=app_path)
        

    def test_create_runners(self):
        '''sanity check launcher.create_new_runners()
        Don't test implementation details here; just ensuring consistency'''
        num_workers = len(self.worker_group)
        num_nodes = sum(w.num_nodes for w in self.worker_group)
        num_ranks = sum(w.num_nodes*w.max_ranks_per_node for w in
                        self.worker_group)
        max_rpn = self.worker_group[0].max_ranks_per_node

        num_serialjobs = random.randint(0, num_ranks+2)
        num_mpijobs = random.randint(0, num_workers+2)
        serialjobs = []
        mpijobs = []

        # Create a big shuffled assortment of jobs
        runner_group = runners.RunnerGroup(Lock())
        for i in range(num_serialjobs):
                job = create_job(name=f"serial{i}", app=self.serialapp.name,
                                 args=str(i), state='PREPROCESSED')
                serialjobs.append(job)
        for i in range(num_mpijobs):
                job = create_job(name=f"mpi{i}", app=self.mpiapp.name,
                                 num_nodes=random.randint(1, num_nodes),
                                 ranks_per_node=random.randint(2, max_rpn),
                                 state='PREPROCESSED')
                mpijobs.append(job)

        all_jobs = serialjobs + mpijobs
        random.shuffle(all_jobs)

        # None are running yet!
        running_pks = runner_group.running_job_pks
        self.assertListEqual(running_pks, [])

        # Invoke create_new_runners once
        # Some set of jobs will start running under control of the RunnerGroup
        # Nondeterministic, due to random() used above, but we just want to
        # check for consistency
        create_new_runners(all_jobs, runner_group, self.worker_group)

        # Get the list of running PKs from the RunnerGroup
        # At least some jobs are running nwo
        running_pks = runner_group.running_job_pks
        self.assertGreater(len(running_pks), 0)
        running_jobs = list(BalsamJob.objects.filter(pk__in=running_pks))
        self.assertGreater(len(running_jobs), 0)

        # Make sure that the aggregate runner PKs agree with the RunnerGroup
        pks_from_runners = [UUID(pk) for runner in runner_group for pk in
                            runner.jobs_by_pk]
        self.assertListEqual(sorted(running_pks), sorted(pks_from_runners))

        # Make sure that the busy workers are correctly marked not idle
        busy_workers = [worker for runner in runner_group for worker in
                        runner.worker_list]
        self.assertTrue(all(w.idle == False for w in busy_workers))

        # And the worker instances in each Runner are the same as the worker
        # instances maintained in the calling code
        busy_workers_ids = [id(w) for w in self.worker_group 
                            if w in busy_workers]
        self.assertListEqual(sorted(busy_workers_ids),
                             sorted([id(w) for w in busy_workers]))

        # Workers not busy are still idle
        self.assertTrue(all(w.idle == True for w in self.worker_group
                            if w not in busy_workers))

        # Now let all the jobs finish
        # Update and remove runners with update_and_remove_finished()
        def check_done():
            runner_group.update_and_remove_finished()
            return all(r.finished() for r in runner_group)

        poll_until_returns_true(check_done, timeout=40)

        # Now there should be no runners, PKs, or busy workers left
        self.assertListEqual(list(runner_group), [])
        self.assertListEqual(runner_group.running_job_pks, [])
        self.assertTrue(all(w.idle==True for w in self.worker_group))

        # And all of the jobs that started running are now marked RUN_DONE
        finished_jobs = list(BalsamJob.objects.filter(pk__in=running_pks))
        self.assertTrue(all(j.state == 'RUN_DONE' for j in finished_jobs))
