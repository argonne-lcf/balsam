"""APIClient-driven tests"""
from rest_framework import status
from balsam.server.models import User

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

    def assertHistory(self, job, *states):
        eventlog = self.client.get_data(
            "job-event-list", uri={"job_id": job["pk"]}, check=status.HTTP_200_OK
        )
        self.assertEqual(eventlog["count"], len(states) - 1)
        for i, (from_state, to_state) in enumerate(zip(states[:-1], states[1:])):
            self.assertDictContainsSubset(
                {"from_state": from_state, "to_state": to_state}, eventlog["results"][i]
            )

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
                parameters={"name": "foo", "N": i, "Name1": 99},
                workdir=f"test/{i}",
            )
            for i in range(1)
        ]
        response = self.create_jobs(jobs, check=status.HTTP_400_BAD_REQUEST)
        self.assertIn("extraneous parameters", str(response))
        jobs = [
            self.job_dict(
                app=self.default_app, parameters={"name": "foo"}, workdir=f"test/{i}",
            )
            for i in range(1)
        ]
        response = self.create_jobs(jobs, check=status.HTTP_400_BAD_REQUEST)
        self.assertIn("missing parameters", str(response))

    def test_added_job_with_parents_is_AWAITING(self):
        pass

    def test_add_job_with_transfers_is_READY(self):
        """Validate stage-in and stage-out items"""
        pass

    def test_add_job_with_two_backends_is_READY(self):
        pass

    def test_cannot_create_job_with_invalid_resources(self):
        pass

    def test_acquire_unbound_for_stage_in(self):
        pass

    def test_acquire_unbound_sorts_already_bound_jobs_first(self):
        pass

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

    def test_can_set_data_and_return_code_on_locked_job(self):
        pass

    def test_can_read_locked_status(self):
        """read-only LOCKED_STATUS appears on Jobs when locked"""
        pass

    def test_concurrent_acquires_for_launch(self):
        pass

    def test_tick_heartbeat_extends_expiration(self):
        pass

    def test_view_session_list(self):
        pass

    def test_delete_session(self):
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
