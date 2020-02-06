import unittest,os,distutils,stat,time
from balsam.platform.scheduler import CobaltScheduler
from balsam.platform.scheduler.dummy import DummyScheduler

class SchedulerTestMixin(object):

    def test_submit(self):

        self.assertTrue(os.path.exists(self.script_path))
        self.assertTrue(os.path.exists(distutils.spawn.find_executable(self.scheduler.submit_exe)))

        job_id = self.scheduler.submit(**self.submit_params)

        self.assertIsInstance(job_id,int)
        self.assertGreater(job_id,0)

        self.scheduler.delete_job(job_id)

        stats = self.scheduler.get_statuses(**self.status_params)
        while job_id in stats:
            time.sleep(1)
            stats = self.scheduler.get_statuses(**self.status_params)

    def test_get_statuses(self):

        self.assertTrue(os.path.exists(distutils.spawn.find_executable(self.scheduler.status_exe)))

        stat_dict = self.scheduler.get_statuses(**self.status_params)

        # check that output is a dictionary
        self.assertIsInstance(stat_dict,dict)

        # check that all states are expected
        balsam_job_states = self.scheduler.job_states.values()
        for id,job_status in stat_dict.items():
            self.assertIsInstance(job_status['project'],str)
            self.assertIsInstance(job_status['queue'],str)
            self.assertIsInstance(job_status['nodes'],int)
            self.assertIsInstance(job_status['wall_time_min'],int)

            self.assertIn(job_status['state'],balsam_job_states)
            self.assertGreaterEqual(job_status['wall_time_min'],0)
            self.assertGreater(job_status['nodes'],0)

    def test_delete_job(self):
        self.assertTrue(os.path.exists(distutils.spawn.find_executable(self.scheduler.delete_exe)))
        self.assertTrue(os.path.exists(self.script_path))
        self.assertTrue(os.path.exists(distutils.spawn.find_executable(self.scheduler.submit_exe)))

        # submit a job
        job_id = self.scheduler.submit(**self.submit_params)

        stdout = self.scheduler.delete_job(job_id)


        self.assertIsInstance(stdout,str)

    def test_get_site_nodelist(self):
        self.assertTrue(os.path.exists(distutils.spawn.find_executable(self.scheduler.nodelist_exe)))

        nodelist = self.scheduler.get_site_nodelist()
        self.assertIsInstance(nodelist,dict)
        self.assertGreater(len(nodelist),0)

        node_states = self.scheduler.node_states.values()

        for id,node_status in nodelist.items():
            self.assertIsInstance(node_status['state'],str)
            self.assertIsInstance(node_status['backfill_time'],int)
            self.assertIsInstance(node_status['queues'],list)

            self.assertGreaterEqual(node_status['backfill_time'],0)

            self.assertIn(node_status['state'],node_states)


class DummyTest(SchedulerTestMixin,unittest.TestCase):
    submit_script = '''#!/usr/bin/env bash
echo [$SECONDS] running $0 $*
echo [$SECONDS] exiting local test script
echo [$SECONDS] JOBID=4
'''
    submit_script_fn = 'dummy_sumbit.sh'

    def setUp(self):
        self.scheduler = DummyScheduler()

        self.script_path = os.path.join(os.getcwd(),self.submit_script_fn)
        ds = open(self.script_path,'w')
        ds.write(self.submit_script)
        ds.close()

        self.submit_params = {
            'script_path': self.script_path,
            'project': 'local_project',
            'queue': 'local_queue',
            'num_nodes': 5,
            'time_minutes': 50,
        }
        self.status_params = {
            'user': 'tiberius',
            'project': None,
            'queue': None,
        }

    def tearDown(self):
        os.remove(self.submit_script_fn)


class CobaltTest(SchedulerTestMixin,unittest.TestCase):

    submit_script = '''!#/usr/bin/env bash
echo [$SECONDS] Running test submit script
echo [$SECONDS] COBALT_JOBID = $COBALT_JOBID
echo [$SECONDS] All Done! Great Test!
'''
    submit_script_fn = 'cobalt_submit.sh'

    def setUp(self):
        self.scheduler = CobaltScheduler()

        self.script_path = os.path.join(os.getcwd(), self.submit_script_fn)
        script = open(self.script_path, 'w')
        script.write(self.submit_script)
        script.close()
        st = os.stat(self.script_path)
        os.chmod(self.script_path, st.st_mode | stat.S_IEXEC)

        self.submit_params = {
            'script_path': self.script_path,
            'project': 'datascience',
            'queue': 'debug-flat-quad',
            'num_nodes': 1,
            'time_minutes': 10,
        }

        self.status_params = {
            'user': os.environ.get('USER','UNKNOWN_USER'),
            'project': None,
            'queue': None,
        }

    def tearDown(self):
        os.remove(self.submit_script_fn)
        log_base = os.path.basename(os.path.splitext(self.script_path)[0])
        if os.path.exists(log_base + '.output'):
            os.remove(log_base + '.output')
        if os.path.exists(log_base + '.error'):
            os.remove(log_base + '.error')
        if os.path.exists(log_base + '.cobaltlog'):
            os.remove(log_base + '.cobaltlog')


if __name__ == '__main__':
    unittest.main()
