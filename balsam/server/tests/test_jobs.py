"""APIClient-driven tests"""
import uuid
from datetime import datetime, timedelta
import pytz
import random
from rest_framework import status
from balsam.server.models import User, Site, AppBackend, Job, BatchJob, EventLog

from .mixins import (
    SiteFactoryMixin,
    BatchJobFactoryMixin,
    AppFactoryMixin,
    JobFactoryMixin,
)
from .clients import (
    BalsamAPIClient,
    TestCase,
)


class JobTests(
    TestCase, SiteFactoryMixin, AppFactoryMixin, BatchJobFactoryMixin, JobFactoryMixin
):
    @classmethod
    def setUpTestData(cls):
        """Called once per entire class! Don't modify users"""
        cls.user = User.objects.create_user(
            username="user", email="user@aol.com", password="abc"
        )

    def setUp(self):
        """Called before each test"""
        self.client = BalsamAPIClient(self)
        self.client.login(username="user", password="abc")
        self.site = self.create_site(hostname="site1")
        self.default_app = self.create_app(sites=self.site, cls_names="DemoApp.hello",)

    def assertHistory(self, job, *states, **expected_messages):
        """
        Assert that `job` went through the sequence of `states` in order.
        For each state:str pair in `expected_messages`, verify the str is contained
        in the transition log message.
        """
        response = self.client.get_data(
            "job-event-list", uri={"job_id": job["pk"]}, check=status.HTTP_200_OK
        )
        self.assertEqual(response["count"], len(states) - 1)
        eventlogs = response["results"]

        for i, (from_state, to_state) in enumerate(zip(states[:-1], states[1:])):
            expected_dict = {"from_state": from_state, "to_state": to_state}
            event = eventlogs[i]
            actual = {key: event[key] for key in ("from_state", "to_state")}
            self.assertDictEqual(expected_dict, actual, msg=actual)
            if to_state in expected_messages:
                self.assertIn(expected_messages.pop(to_state), event["message"])

    def setup_two_site_scenario(self, num_jobs):
        self.site1 = self.create_site(hostname="siteX")
        self.site2 = self.create_site(hostname="siteY")
        self.session1 = self.create_session(self.site1, label="Site 1 session")
        self.session2 = self.create_session(self.site2, label="Site 2 session")
        self.dual_site_app = self.create_app(
            sites=[self.site1, self.site2],
            cls_names=["demo.Hello", "demo.Hello"],
            name="Demo",
        )
        specs = [
            self.job_dict(app=self.dual_site_app, workdir=f"./test/{i}")
            for i in range(num_jobs)
        ]
        jobs = self.create_jobs(specs, check=status.HTTP_201_CREATED)
        return jobs

    def setup_one_site_two_launcher_scenario(self, num_jobs):
        site = Site.objects.get(pk=self.site["pk"])
        self.bjob1, self.bjob2 = [
            BatchJob.objects.create(
                site,
                project="datascience",
                queue="default",
                num_nodes=128,
                wall_time_min=30,
                job_mode="mpi",
                filter_tags={},
            )
            for i in range(2)
        ]
        self.session1 = self.create_session(
            self.site, label="Launcher 1", batch_job=self.bjob1.pk
        )
        self.session2 = self.create_session(
            self.site, label="Launcher 2", batch_job=self.bjob2.pk
        )
        specs = [
            self.job_dict(app=self.default_app, workdir=f"./test/{i}")
            for i in range(num_jobs)
        ]
        jobs = self.create_jobs(specs, check=status.HTTP_201_CREATED)
        return jobs

    def setup_varying_resources_scenario(self, *specs):
        """Create many runnable jobs with varying resource requirements"""
        site = Site.objects.get(pk=self.site["pk"])
        self.bjob = BatchJob.objects.create(
            site,
            project="datascience",
            queue="default",
            num_nodes=128,
            wall_time_min=30,
            job_mode="mpi",
            filter_tags={},
        )
        self.session = self.create_session(
            self.site, label="Launcher 1", batch_job=self.bjob.pk
        )
        jobs = []
        for count, spec in specs:
            d = self.job_dict(
                num_nodes=spec.get("num_nodes", 1),
                ranks_per_node=spec.get("ranks_per_node", 1),
                threads_per_rank=spec.get("threads_per_rank", 1),
                threads_per_core=spec.get("threads_per_core", 1),
                gpus_per_rank=spec.get("gpus_per_rank", 0),
                node_packing_count=spec.get("node_packing_count", 1),
                wall_time_min=spec.get("wall_time_min", 0),
            )
            jobs += count * [d]
        jobs = self.create_jobs(jobs)
        Job.objects.bulk_update(
            [{"pk": j["pk"], "state": "PREPROCESSED"} for j in jobs]
        )
        return

    def test_add_job(self):
        """One backend, no parents, no transfers: straight to STAGED_IN"""
        jobs = [
            self.job_dict(
                app=self.default_app,
                parameters={"name": "foo", "N": i},
                workdir=f"test/{i}",
            )
            for i in range(3)
        ]
        jobs = self.create_jobs(jobs, check=status.HTTP_201_CREATED)
        for job in jobs:
            self.assertEqual(job["state"], "STAGED_IN")
            self.assertHistory(job, "CREATED", "READY", "STAGED_IN")

    def test_bad_job_parameters_refused(self):
        jobs = [
            self.job_dict(
                app=self.default_app,
                parameters={"name": "foo", "N": 0, "Name1": 99},
                workdir=f"test/0",
            )
        ]
        response = self.create_jobs(jobs, check=status.HTTP_400_BAD_REQUEST)
        self.assertIn("extraneous parameters", str(response))
        jobs = [
            self.job_dict(
                app=self.default_app, parameters={"name": "foo"}, workdir=f"test/0",
            )
        ]
        response = self.create_jobs(jobs, check=status.HTTP_400_BAD_REQUEST)
        self.assertIn("missing parameters", str(response))

    def test_added_job_with_parents_is_AWAITING(self):
        parent = self.create_jobs(self.job_dict())
        child = self.create_jobs(self.job_dict(parents=[parent["pk"]]))
        self.assertEqual(parent["state"], "STAGED_IN")
        self.assertEqual(child["state"], "AWAITING_PARENTS")

    def test_add_job_with_bad_globus_uuids(self):
        """Validate stage-in and stage-out items"""
        job_spec = self.job_dict(
            transfers=[
                dict(
                    source="globus://afaf/path/to/x", destination="./x2", direction="in"
                )
            ]
        )
        response = self.create_jobs(job_spec, check=status.HTTP_400_BAD_REQUEST)
        self.assertIn("badly formed hexadecimal UUID string", response)

    def test_add_job_with_transfers_is_READY(self):
        """Ready and bound to backend"""
        gid = uuid.uuid4()
        job_spec = self.job_dict(
            transfers=[
                dict(
                    source=f"globus://{gid}/path/to/x",
                    destination="./x2",
                    direction="in",
                )
            ]
        )
        job = self.create_jobs(job_spec, check=status.HTTP_201_CREATED)
        self.assertEqual(job["state"], "READY")
        self.assertEqual(job["site"], str(Site.objects.first()))
        self.assertEqual(job["app_class"], AppBackend.objects.first().class_name)

    def test_added_job_with_two_backends_is_READY_but_unbound(self):
        site1 = self.create_site(hostname="siteX")
        site2 = self.create_site(hostname="siteY")
        two_backend_app = self.create_app(
            sites=[site1, site2], cls_names=["demo.Hello", "demo.Hello"], name="Demo"
        )
        job_spec = self.job_dict(app=two_backend_app)
        job = self.create_jobs(job_spec, check=status.HTTP_201_CREATED)
        self.assertEqual(job["state"], "READY")

        # site & app_class of None signify job is unbound
        self.assertEqual(job["site"], None)
        self.assertEqual(job["app_class"], None)

    def test_cannot_create_job_with_invalid_resources(self):
        # num_nodes at least 1
        self.create_jobs(self.job_dict(num_nodes=0), check=status.HTTP_400_BAD_REQUEST)
        # wall_time_min of 0 is ok
        self.create_jobs(self.job_dict(wall_time_min=0), check=status.HTTP_201_CREATED)

    def test_acquire_unbound_for_stage_in(self):
        all_jobs = self.setup_two_site_scenario(num_jobs=100)
        self.assertEqual(len(all_jobs), 100)

        # session1 acquires up to 20 jobs
        sess_1_acquired = self.acquire_jobs(
            session=self.session1,
            acquire_unbound=True,
            states=["READY"],
            max_num_acquire=20,
        )
        self.assertEqual(len(sess_1_acquired), 20)
        self.assertSetEqual(set(["READY"]), set(j["state"] for j in sess_1_acquired))

        # session2 acquires up to 500 jobs; can only get the 80 unlocked
        sess_2_acquired = self.acquire_jobs(
            session=self.session2,
            acquire_unbound=True,
            states=["READY"],
            max_num_acquire=500,
        )
        self.assertEqual(len(sess_2_acquired), 80)
        self.assertSetEqual(set(["READY"]), set(j["state"] for j in sess_2_acquired))

    def test_acquire_unbound_sorts_already_bound_jobs_first(self):
        self.setup_two_site_scenario(num_jobs=100)

        # Session1 acquires 50 unbound jobs
        sess_1_acquired = self.acquire_jobs(
            session=self.session1,
            acquire_unbound=True,
            states=["READY"],
            max_num_acquire=50,
        )

        # The first 3 are updated to STAGED_IN: nothing to do.
        updates = [
            {
                "pk": j["pk"],
                "state": "STAGED_IN",
                "state_timestamp": datetime.utcnow(),
                "state_message": "Skipped Stage-In: nothing to do.",
            }
            for j in sess_1_acquired[:3]
        ]
        self.client.bulk_patch_data("job-list", updates, check=status.HTTP_200_OK)

        # the event log looks correct:
        for job in sess_1_acquired[:3]:
            self.assertHistory(
                job, "CREATED", "READY", "STAGED_IN", STAGED_IN="nothing to do"
            )

        # the remaining 47 jobs are still locked, while the first 3 unlocked:
        locked_pks = [j["pk"] for j in sess_1_acquired[3:]]
        for job in Job.objects.all():
            if job.pk in locked_pks:
                self.assertNotEqual(job.lock, None)
            else:
                self.assertEqual(job.lock, None)

        # Then Session1 is suddenly over:
        self.client.delete_data(
            "session-detail",
            uri={"pk": self.session1["pk"]},
            check=status.HTTP_204_NO_CONTENT,
        )

        # All jobs are unlocked now
        # But the 50 that were acquired remain bound to site1
        acquired_pks = [j["pk"] for j in sess_1_acquired]
        for job in Job.objects.all():
            self.assertEqual(job.lock, None)
            if job.pk in acquired_pks:
                self.assertEqual(job.app_backend.site.pk, self.site1["pk"])

        # Site1 starts up another session and acquires 60 jobs:
        sess1_restarted = self.create_session(self.site1)
        sess_1_acquired = self.acquire_jobs(
            session=sess1_restarted,
            acquire_unbound=True,
            states=["READY"],
            max_num_acquire=60,
            order_by=["-wall_time_min"],
        )

        # Accounting for the 100 jobs:
        # The first 47 READY jobs are the already-acquired ones (order matters!)
        # 3 (not acquired in this query) are STAGED_IN
        # 13 are newly acquired
        # The last 37 are still unbound
        for job in sess_1_acquired[:47]:
            self.assertIn(job["pk"], acquired_pks)
        for job in sess_1_acquired[47:]:
            self.assertNotIn(job["pk"], acquired_pks)
        self.assertEqual(Job.objects.filter(app_backend__isnull=True).count(), 37)

        # When session2 tries to acquire 1000 jobs, it therefore only gets 37:
        sess_2_acquired = self.acquire_jobs(
            session=self.session2,
            acquire_unbound=True,
            states=["READY"],
            max_num_acquire=1000,
        )
        self.assertEqual(len(sess_2_acquired), 37)

    def test_acquire_bound_only_for_transitions(self):
        self.setup_two_site_scenario(num_jobs=10)

        # Nothing can be acquired for pre/post-processing, because it's unbound:
        acquired = self.acquire_jobs(
            session=self.session1,
            acquire_unbound=False,
            states=["STAGED_IN", "RUN_DONE", "RUN_ERROR", "RESTART_READY"],
            max_num_acquire=20,
        )
        self.assertEqual(len(acquired), 0)

        # Acquire unbound & Update to STAGED_IN
        acquired = self.acquire_jobs(
            session=self.session1,
            acquire_unbound=True,
            states=["READY"],
            max_num_acquire=20,
        )
        self.assertEqual(len(acquired), 10)
        updates = [{"pk": j["pk"], "state": "STAGED_IN"} for j in acquired]
        self.client.bulk_patch_data("job-list", updates, check=status.HTTP_200_OK)

        # Now the preprocess module can acquire all the jobs:
        acquired = self.acquire_jobs(
            session=self.session1,
            acquire_unbound=False,
            states=["STAGED_IN", "RUN_DONE", "RUN_ERROR", "RESTART_READY"],
            max_num_acquire=20,
        )
        self.assertEqual(len(acquired), 10)

    def test_acquire_validates_state_list(self):
        """Cannot provide an invalid state to acquire"""
        self.setup_two_site_scenario(num_jobs=2)
        acquired_jobs = self.acquire_jobs(
            session=self.session1,
            acquire_unbound=True,
            states=["READY", "GOOF"],
            max_num_acquire=100,
            check=status.HTTP_400_BAD_REQUEST,
        )
        self.assertEqual(acquired_jobs, None)

    def test_acquire_for_launch(self):
        """Jobs become associated with BatchJob"""
        jobs = self.setup_one_site_two_launcher_scenario(num_jobs=10)

        # At first, the jobs have no batch_job because they're not running yet
        for job in jobs:
            self.assertEqual(job["state"], "STAGED_IN")
            self.assertEqual(job["batch_job"], None)

        # Mark jobs PREPROCESSED and ready to run
        pks = [j["pk"] for j in jobs]
        Job.objects.bulk_update([{"pk": pk, "state": "PREPROCESSED"} for pk in pks])

        # Launcher1 acquires 2 runnable jobs:
        acquired1 = self.acquire_jobs(
            session=self.session1,
            acquire_unbound=False,
            states=["PREPROCESSED", "RESTART_READY"],
            max_num_acquire=2,
        )

        # Launcher2 asks for up to 1000 and gets all the rest:
        acquired2 = self.acquire_jobs(
            session=self.session2,
            acquire_unbound=False,
            states=["PREPROCESSED", "RESTART_READY"],
            max_num_acquire=1000,
        )

        # These jobs are now associated with the BatchJob that launcher1 runs under
        for job in acquired1:
            self.assertEqual(job["batch_job"], self.bjob1.pk)

        for job in acquired2:
            self.assertEqual(job["batch_job"], self.bjob2.pk)

    def test_update_to_running_does_not_release_lock(self):
        jobs = self.setup_one_site_two_launcher_scenario(num_jobs=10)

        # Mark jobs PREPROCESSED
        pks = [j["pk"] for j in jobs]
        Job.objects.bulk_update([{"pk": pk, "state": "PREPROCESSED"} for pk in pks])

        # Launcher1 acquires all of them
        acquired = self.acquire_jobs(
            session=self.session1,
            acquire_unbound=False,
            states=["PREPROCESSED", "RESTART_READY"],
            max_num_acquire=10,
        )

        # Behind the scenes, the acquired jobs are locked:
        for job in Job.objects.all():
            self.assertEqual(job.lock_id, self.session1["pk"])

        # The jobs start running in a staggered fashion; a bulk status update is made
        run_start_times = [
            (datetime.utcnow() + timedelta(seconds=random.randint(0, 20))).replace(
                tzinfo=pytz.UTC
            )
            for i in range(len(acquired))
        ]
        updates = [
            {
                "pk": j["pk"],
                "state": "RUNNING",
                "state_timestamp": ts,
                "state_message": "Running on Theta nid00139",
            }
            for j, ts in zip(acquired, run_start_times)
        ]
        jobs = self.client.bulk_patch_data(
            "job-list", updates, check=status.HTTP_200_OK
        )

        # The jobs are associated to batchjob and have the expected history:
        for job in jobs:
            self.assertEqual(job["batch_job"], self.bjob1.pk)
            self.assertHistory(
                job, "CREATED", "READY", "STAGED_IN", "PREPROCESSED", "RUNNING"
            )

        # Behind the scenes, the acquired jobs have changed state and are still locked:
        for job in Job.objects.all():
            self.assertEqual(job.lock_id, self.session1["pk"])

        # The EventLogs were correctly recorded:
        time_stamps = list(
            EventLog.objects.filter(to_state="RUNNING").values_list(
                "timestamp", flat=True
            )
        )
        self.assertSetEqual(set(run_start_times), set(time_stamps))

    def test_acquire_for_launch_with_node_constraints(self):
        # DB has:
        self.setup_varying_resources_scenario(
            (2, dict(num_nodes=3, wall_time_min=0)),
            (5, dict(num_nodes=1, wall_time_min=0)),
            (5, dict(num_nodes=1, wall_time_min=30)),
        )

        # Scenario: 7 idle nodes & 20 minutes left
        resources = {
            "max_jobs_per_node": 1,
            "max_wall_time_min": 20,
            "running_job_counts": [1, 1] + 7 * [0] + [1, 1, 1],
            "node_occupancies": [1, 1] + 7 * [0] + [1, 1, 1],
            "idle_cores": [63, 63] + 7 * [64] + [63, 63, 63],
            "idle_gpus": [0] * 12,
        }

        # Expected behavior: 2 3-node jobs & 1 1-node job are acquired: in *That* order
        acquired = self.acquire_jobs(
            session=self.session,
            acquire_unbound=False,
            states=["PREPROCESSED", "RESTART_READY"],
            max_num_acquire=7,
            node_resources=resources,
            order_by=["-num_nodes", "-wall_time_min"],
        )
        self.assertEqual(len(acquired), 3)
        self.assertEqual(acquired[0]["num_nodes"], 3)
        self.assertEqual(acquired[1]["num_nodes"], 3)
        self.assertEqual(acquired[2]["num_nodes"], 1)
        self.assertListEqual([j["wall_time_min"] for j in acquired], 3 * [0])

    def test_acquire_for_launch_respects_provided_ordering(self):
        self.setup_varying_resources_scenario(
            *[
                (
                    1,
                    dict(
                        num_nodes=random.randint(1, 4),
                        wall_time_min=random.randint(0, 60),
                    ),
                )
                for i in range(100)
            ]
        )

        resources = {
            "max_jobs_per_node": 1,
            "max_wall_time_min": 20,
            "running_job_counts": 128 * [0],
            "node_occupancies": 128 * [0],
            "idle_cores": 128 * [1],
            "idle_gpus": 128 * [0],
        }

        acquired = self.acquire_jobs(
            session=self.session,
            acquire_unbound=False,
            states=["PREPROCESSED", "RESTART_READY"],
            max_num_acquire=128,
            node_resources=resources,
            order_by=["-num_nodes", "-wall_time_min"],
        )
        nodes_minutes = [(job["num_nodes"], job["wall_time_min"]) for job in acquired]
        self.assertEqual(nodes_minutes, sorted(nodes_minutes, reverse=True))
        self.assertLessEqual(max(n[1] for n in nodes_minutes), 20)
        self.assertLessEqual(sum(n[0] for n in nodes_minutes), 128)

    def test_acquire_for_launch_respects_idle_core_limits(self):
        resources = {
            "max_jobs_per_node": 16,
            "max_wall_time_min": 20,
            "running_job_counts": [2, 1],
            "node_occupancies": [2.0 / 16, 1.0 / 16],
            "idle_cores": [5, 7],
            "idle_gpus": [0, 0],
        }
        self.setup_varying_resources_scenario(
            (
                2,
                dict(
                    num_nodes=1,
                    threads_per_rank=2,
                    threads_per_core=1,
                    node_packing_count=16,
                ),
            ),
            (
                8,
                dict(
                    num_nodes=1,
                    threads_per_rank=4,
                    threads_per_core=2,
                    node_packing_count=8,
                ),
            ),
        )
        acquired = self.acquire_jobs(
            session=self.session,
            acquire_unbound=False,
            states=["PREPROCESSED", "RESTART_READY"],
            max_num_acquire=128,
            node_resources=resources,
            order_by=["-num_nodes", "-wall_time_min", "node_packing_count"],
        )
        packing_counts = [job["node_packing_count"] for job in acquired]
        self.assertEqual(len(acquired), 5)
        self.assertListEqual(packing_counts, 5 * [8])

    def test_acquire_for_launch_respects_node_packing_counts(self):
        resources = {
            "max_jobs_per_node": 16,
            "max_wall_time_min": 20,
            "running_job_counts": [0, 0, 0],
            "node_occupancies": [0, 0, 0],
            "idle_cores": [8, 8, 8],
            "idle_gpus": [0, 0, 0],
        }
        self.setup_varying_resources_scenario(
            (8, dict(num_nodes=1, node_packing_count=4)),
            (4, dict(num_nodes=1, node_packing_count=2)),
        )
        acquired = self.acquire_jobs(
            session=self.session,
            acquire_unbound=False,
            states=["PREPROCESSED", "RESTART_READY"],
            max_num_acquire=128,
            node_resources=resources,
            order_by=["-num_nodes", "-wall_time_min", "node_packing_count"],
        )
        packing_counts = [job["node_packing_count"] for job in acquired]
        self.assertEqual(len(acquired), 8)
        self.assertListEqual(packing_counts, 4 * [2] + 4 * [4])

    def test_acquire_for_launch_respects_gpu_limits(self):
        resources = {
            "max_jobs_per_node": 16,
            "max_wall_time_min": 20,
            "running_job_counts": [0, 0, 0],
            "node_occupancies": [0, 0, 0],
            "idle_cores": [8, 8, 8],
            "idle_gpus": [1, 1, 1],
        }
        self.setup_varying_resources_scenario(
            (10, dict(num_nodes=1, node_packing_count=16, gpus_per_rank=1)),
        )
        acquired = self.acquire_jobs(
            session=self.session,
            acquire_unbound=False,
            states=["PREPROCESSED", "RESTART_READY"],
            max_num_acquire=128,
            node_resources=resources,
            order_by=["-num_nodes", "-wall_time_min", "node_packing_count"],
        )
        self.assertEqual(len(acquired), 3)

    def test_acquire_for_launch_colocates_MPI_and_single_node_tasks(self):
        """First job: 1 rpn, 128 nodes, packing_count=64.  2048 jobs: 1-node, single-rank, packing_count=2 and using 1 GPU"""
        job_size = 16
        resources = {
            "max_jobs_per_node": 2,
            "max_wall_time_min": 30,
            "running_job_counts": job_size * [0],
            "node_occupancies": job_size * [0],
            "idle_cores": job_size * [64],
            "idle_gpus": job_size * [6],
        }

        self.setup_varying_resources_scenario(
            (1, dict(num_nodes=job_size, node_packing_count=64, gpus_per_rank=0)),
            (2 * job_size, dict(num_nodes=1, node_packing_count=2, gpus_per_rank=1),),
        )
        acquired = self.acquire_jobs(
            session=self.session,
            acquire_unbound=False,
            states=["PREPROCESSED", "RESTART_READY"],
            max_num_acquire=1024,
            node_resources=resources,
            order_by=["-num_nodes", "-wall_time_min", "node_packing_count"],
        )
        self.assertEqual(len(acquired), 1 + job_size)
        self.assertEqual(acquired[0]["num_nodes"], job_size)

    def test_bulk_update_based_on_tags_filter_via_put(self):
        pass

    def test_can_filter_on_parameters(self):
        pass

    def test_can_filter_on_data(self):
        pass

    def test_can_filter_on_parents__in(self):
        pass

    def test_can_filter_on_app_name(self):
        pass

    def test_can_filter_on_last_update(self):
        pass

    def test_update_to_run_done_releases_lock_but_not_batch_job(self):
        pass

    def test_can_set_data_and_return_code_on_locked_job(self):
        pass

    def test_can_read_locked_status(self):
        """read-only LOCKED_STATUS appears on Jobs when locked"""
        pass

    def test_concurrent_acquires_for_launch(self):
        pass

    def test_tick_heartbeat_extends_expiration(self):
        pass

    def test_tick_heartbeat_clears_expired_locks(self):
        pass

    def test_view_session_list(self):
        pass

    def test_delete_session_frees_lock_on_all_jobs(self):
        pass

    def test_update_transfer_items_on_locked_job(self):
        """Can update state, status_message, task_id"""
        pass

    def test_cannot_alter_transfer_item_source_or_dest(self):
        pass

    # List & Filtering Jobs
    def test_filter_jobs_by_data(self):
        pass

    def test_filter_jobs_by_tags(self):
        pass

    def test_filter_jobs_by_state(self):
        pass

    def test_can_traverse_dag(self):
        pass

    # Viewing State History
    def test_aggregated_state_history(self):
        pass

    def test_aggregated_state_history_by_batch_job(self):
        pass

    def test_aggregated_state_history_by_tags(self):
        pass

    def test_aggregated_state_history_by_date_range(self):
        pass

    def test_cannot_delete_locked_job(self):
        pass

    def test_can_delete_unlocked_job(self):
        pass

    def test_delete_recursively_deletes_children(self):
        pass

    def test_disallow_invalid_transition(self):
        pass

    def test_resource_change_records_provenance_event_log(self):
        pass

    def test_cannot_acquire_with_another_lock_id(self):
        """Passing a lock id that belongs to another user results in acquire() error"""
        pass

    def test_finished_job_triggers_children_ready_and_unbound(self):
        """Job with one child FINISHED; child is READY but unbound (has 2 backends)"""
        pass

    def test_finished_job_triggers_children_staged_in(self):
        """Job with one child FINISHED; child goes all the way to STAGED_IN"""
        pass

    def test_finished_job_triggers_children_ready(self):
        """Job with one child FINISHED; child goes to READY because it has transfers"""
        pass

    def test_child_with_two_parents_still_waiting_when_one_parent_finished(self):
        pass

    def test_child_with_two_parents_ready_when_both_finished(self):
        pass

    def test_postprocessed_job_cascades_to_finished(self):
        """POSTPROCESSED job without stage-outs goes to FINISHED"""
        pass

    def test_postprocessed_job_with_stage_outs(self):
        """POSTPROCESSED job is acquired for stage-out before marking FINISHED"""
        pass

    def test_reset_job_with_parents_goes_to_awaiting(self):
        pass

    def test_reset_job_with_transfers_goes_to_ready(self):
        pass

    def test_reset_job_with_two_backends_becomes_unbound(self):
        pass

    def test_run_error__update_releases_lock_and_records_last_error(self):
        pass
