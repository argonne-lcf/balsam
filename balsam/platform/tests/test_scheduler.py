import unittest,os
from balsam.platform.scheduler import DummyScheduler

class SchedulerTestMixin(object):

    def test_submit(self):

        job_id = self.scheduler.submit(
            self.script_path,
            self.project,
            self.queue,
            self.num_nodes,
            self.time_minutes)

        self.assertIsInstance(job_id,int)
        self.assertGreater(job_id,0)

    def test_get_statuses(self):

        stat_dict = self.scheduler.get_statuses(self.project, self.user, self.queue)

        # check that output is a dictionary
        self.assertIsInstance(stat_dict,dict)

        # check that all states are expected
        balsam_job_states = DummyScheduler.job_states.values()
        for id,job_status in stat_dict.items():
            self.assertIsInstance(job_status['project'],str)
            self.assertIsInstance(job_status['queue'],str)
            self.assertIsInstance(job_status['nodes'],int)
            self.assertIsInstance(job_status['wall_time_min'],int)

            self.assertIn(job_status['state'],balsam_job_states)
            self.assertGreaterEqual(job_status['wall_time_min'],0)
            self.assertGreater(job_status['nodes'],0)


    def test_delete_job(self):

        stdout = self.scheduler.delete_job(self.delete_job_id)

        self.assertIsInstance(stdout,str)

    def test_get_site_nodelist(self):

        nodelist = self.scheduler.get_site_nodelist()
        self.assertIsInstance(nodelist,dict)
        self.assertGreater(len(nodelist),0)

        node_states = DummyScheduler.node_states.values()

        for id,node_status in nodelist.items():
            self.assertIsInstance(node_status['node_state'],str)
            self.assertIsInstance(node_status['backfill_time'],int)
            self.assertIsInstance(node_status['queues'],list)

            self.assertGreaterEqual(node_status['backfill_time'],0)

            self.assertIn(node_status['node_state'],node_states)


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

        self.project = 'local_project'
        self.queue = 'local_queue'
        self.num_nodes = 5
        self.time_minutes = 50
        self.user = 'tiberius'
        self.delete_job_id = 5

    def tearDown(self):
        os.remove(self.submit_script_fn)


# class CobaltTest(SchedulerTest):
#     def setUpClass(self):
#         self.scheduler = CobaltScheduler()



if __name__ == '__main__':
    unittest.main()
