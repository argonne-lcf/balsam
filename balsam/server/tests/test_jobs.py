"""APIClient-driven tests"""
import uuid
from datetime import datetime
from rest_framework import status
from balsam.server.models import User, Site, AppBackend, Job

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

    def test_acquire_bound_for_transitions(self):
        pass

    def test_acquire_for_launch(self):
        """Jobs become associated with BatchJob"""
        pass

    def test_acquire_for_launch_with_node_constraints(self):
        """Jobs become associated with BatchJob"""
        pass

    def test_update_to_running_does_not_release_lock(self):
        pass

    def test_bulk_update_based_on_tags_filter_via_put(self):
        pass

    def test_bulk_status_update_via_patch(self):
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

    def test_disallow_invalid_transition(self):
        pass

    def test_resource_change_records_provenance_event_log(self):
        pass

    def test_cannot_acquire_with_another_lock_id(self):
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
