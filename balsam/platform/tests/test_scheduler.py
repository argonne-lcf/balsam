import os
import shutil
import stat
import unittest
import time

from balsam.platform.scheduler import CobaltScheduler, SlurmScheduler
from balsam.platform.scheduler.dummy import DummyScheduler


class SchedulerTestMixin(object):
    def assertInPath(self, exe):
        which_exe = shutil.which(exe)
        self.assertTrue(which_exe is not None, f"'{exe}' not in PATH")

    def test_submit(self):

        # verify script exists
        self.assertTrue(os.path.exists(self.script_path))
        # verify submit command is in path
        self.assertInPath(self.scheduler.submit_exe)
        # submit job
        job_id = self.scheduler.submit(**self.submit_params)
        # check job id for expected output
        self.assertIsInstance(job_id, int)
        self.assertGreater(job_id, 0)

        # clean up after this test, delete job, wait for delete to be complete
        self.scheduler.delete_job(job_id)
        stats = self.scheduler.get_statuses(**self.status_params)
        count = 0
        while job_id in stats:
            time.sleep(1)
            stats = self.scheduler.get_statuses(**self.status_params)
            if count > 30:
                break
            count += 1

    def test_get_statuses(self):
        # verify status command is in path
        self.assertInPath(self.scheduler.status_exe)

        # submit job to stat
        job_id = self.scheduler.submit(**self.submit_params)

        # test status function
        stat_dict = self.scheduler.get_statuses(**self.status_params)
        # check that output is a dictionary
        self.assertIsInstance(stat_dict, dict)

        self.assertIn(job_id, stat_dict)

        # check that all states are expected
        balsam_job_states = self.scheduler.job_states.values()
        for id, job_status in stat_dict.items():
            self.assertIsInstance(job_status.project, str)
            self.assertIsInstance(job_status.queue, str)
            self.assertIsInstance(job_status.nodes, int)
            self.assertIsInstance(job_status.wall_time_min, int)

            self.assertIn(job_status.state, balsam_job_states)
            self.assertGreaterEqual(job_status.wall_time_min, 0)
            self.assertGreater(job_status.nodes, 0)

        # clean up after this test, delete job, wait for delete to be complete
        self.scheduler.delete_job(job_id)

    def test_delete_job(self):
        # verify delete command exists
        self.assertInPath(self.scheduler.delete_exe)

        # submit a job for deletion
        job_id = self.scheduler.submit(**self.submit_params)
        # delete the job
        stdout = self.scheduler.delete_job(job_id)
        # validate output
        self.assertIsInstance(stdout, str)

    def test_get_backfill_windows(self):
        # verify nodelist command is in path
        self.assertInPath(self.scheduler.backfill_exe)

        windows = self.scheduler.get_backfill_windows()
        self.assertIsInstance(windows, dict)
        self.assertGreaterEqual(len(windows), 0)

        # verify that nodelist has expected output
        for queue, windows in windows.items():
            for window in windows:
                self.assertIsInstance(window.num_nodes, int)
                self.assertIsInstance(window.backfill_time_min, int)


class DummyTest(SchedulerTestMixin, unittest.TestCase):
    submit_script = """#!/usr/bin/env bash
echo [$SECONDS] running $0 $*
echo [$SECONDS] exiting local test script
echo [$SECONDS] JOBID=4
"""
    submit_script_fn = "dummy_sumbit.sh"

    def setUp(self):
        self.scheduler = DummyScheduler()

        self.script_path = os.path.join(os.getcwd(), self.submit_script_fn)
        ds = open(self.script_path, "w")
        ds.write(self.submit_script)
        ds.close()

        self.submit_params = {
            "script_path": self.script_path,
            "project": "local_project",
            "queue": "local_queue",
            "num_nodes": 5,
            "time_minutes": 50,
        }
        self.status_params = {
            "user": "tiberius",
            "project": None,
            "queue": None,
        }

    def tearDown(self):
        os.remove(self.submit_script_fn)


class CobaltTest(SchedulerTestMixin, unittest.TestCase):

    submit_script = """!#/usr/bin/env bash
echo [$SECONDS] Running test submit script
echo [$SECONDS] COBALT_JOBID = $COBALT_JOBID
echo [$SECONDS] All Done! Great Test!
"""
    submit_script_fn = "cobalt_submit.sh"

    def setUp(self):
        self.scheduler = CobaltScheduler()

        self.script_path = os.path.join(os.getcwd(), self.submit_script_fn)
        script = open(self.script_path, "w")
        script.write(self.submit_script)
        script.close()
        st = os.stat(self.script_path)
        os.chmod(self.script_path, st.st_mode | stat.S_IEXEC)

        self.submit_params = {
            "script_path": self.script_path,
            "project": "datascience",
            "queue": "debug-flat-quad",
            "num_nodes": 1,
            "time_minutes": 10,
        }

        self.status_params = {
            "user": os.environ.get("USER", "UNKNOWN_USER"),
            "project": None,
            "queue": None,
        }

    def tearDown(self):
        os.remove(self.submit_script_fn)
        log_base = os.path.basename(os.path.splitext(self.script_path)[0])
        if os.path.exists(log_base + ".output"):
            os.remove(log_base + ".output")
        if os.path.exists(log_base + ".error"):
            os.remove(log_base + ".error")
        if os.path.exists(log_base + ".cobaltlog"):
            os.remove(log_base + ".cobaltlog")


class SlurmTest(SchedulerTestMixin, unittest.TestCase):

    submit_script = """#!/usr/bin/env bash -l
echo [$SECONDS] Running test submit script
echo [$SECONDS] SLURM_JOB_ID = SLURM_JOB_ID
echo [$SECONDS] All Done! Great Test!
"""
    submit_script_fn = "slurm_submit.sh"

    def setUp(self):
        self.scheduler = SlurmScheduler()
        self.scheduler.default_submit_kwargs = {"constraint": "haswell"}
        self.scheduler.submit_kwargs_flag_map = {"constraint": "-C"}

        self.script_path = os.path.join(os.getcwd(), self.submit_script_fn)
        script = open(self.script_path, "w")
        script.write(self.submit_script)
        script.close()
        st = os.stat(self.script_path)
        os.chmod(self.script_path, st.st_mode | stat.S_IEXEC)

        self.submit_params = {
            "script_path": self.script_path,
            "project": "m3512",
            "queue": "debug",
            "num_nodes": 1,
            "time_minutes": 10,
        }

        self.status_params = {
            "user": os.environ.get("USER", "UNKNOWN_USER"),
            "project": None,
            "queue": None,
        }

    def tearDown(self):
        os.remove(self.submit_script_fn)
        log_base = os.path.basename(os.path.splitext(self.script_path)[0])
        if os.path.exists(log_base + ".output"):
            os.remove(log_base + ".output")
        if os.path.exists(log_base + ".error"):
            os.remove(log_base + ".error")
        if os.path.exists(log_base + ".cobaltlog"):
            os.remove(log_base + ".cobaltlog")


if __name__ == "__main__":
    import logging

    logging.basicConfig()
    unittest.main()
