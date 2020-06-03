from datetime import datetime, timedelta
from dateutil.parser import isoparse
import random
from rest_framework import status
from balsam.server.models import Site, BatchJob

from .mixins import (
    SiteFactoryMixin,
    BatchJobFactoryMixin,
)
from .clients import (
    TestCase,
    TwoUserTestCase,
)


class BatchJobTests(TestCase, SiteFactoryMixin, BatchJobFactoryMixin):
    def test_can_create_batchjob(self):
        site = self.create_site()
        batch_job = self.create_batchjob(site=site, check=status.HTTP_201_CREATED)
        self.assertIn("status_message", batch_job)
        self.assertIn("scheduler_id", batch_job)
        self.assertEqual(batch_job["state"], "pending-submission", msg=batch_job)

    def test_create_with_nested_site_causes_400(self):
        site = self.create_site()
        resp = self.client.post_data(
            "batchjob-list",
            site=site,  # Mistake here: site should be PK int
            project="datascience",
            queue="default",
            num_nodes=128,
            wall_time_min=60,
            job_mode="mpi",
            filter_tags={},
            check=status.HTTP_400_BAD_REQUEST,
        )
        self.assertIn("site must be an integer", str(resp))

    def test_list_batchjobs_spanning_sites(self):
        site1 = self.create_site(hostname="1")
        site2 = self.create_site(hostname="2")
        for time in [10, 20, 30, 40]:
            for site in [site1, site2]:
                self.create_batchjob(site=site, wall_time_min=time)
        bjob_list = self.client.get_data("batchjob-list")
        self.assertEqual(bjob_list["count"], 8)
        self.assertEqual(len(bjob_list["results"]), 8)

    def test_filter_by_site(self):
        site1 = self.create_site(hostname="1")
        site2 = self.create_site(hostname="2")
        for time in [10, 20, 30, 40]:
            for site in [site1, site2]:
                self.create_batchjob(site=site, wall_time_min=time)

        # providing GET kwargs causes result list to be filtered
        dat = self.client.get_data("batchjob-list", site=site2["pk"])
        self.assertEqual(dat["count"], 4)
        results = dat["results"]
        self.assertEqual(len(results), 4)
        self.assertListEqual([j["site"] for j in results], 4 * [site2["pk"]])

    def test_filter_by_time_range(self):
        site = self.create_site()
        # Create 10 historical batchjobs
        # Job n started n hours ago and took 30 minutes
        now = datetime.utcnow()  # IMPORTANT! all times in UTC
        for i in range(1, 11):
            j = self.create_batchjob(site=site, job_mode="serial")
            start = now - timedelta(hours=i * 1)
            end = start + timedelta(minutes=30)
            j.update(state="finished", start_time=start, end_time=end)
            if now - timedelta(hours=5) <= end <= now - timedelta(hours=3):
                j["filter_tags"].update(good="Yes")
            self.client.put_data(
                "batchjob-detail", uri={"pk": j["pk"]}, **j, check=status.HTTP_200_OK
            )

        # Now, we want to filter for jobs that ended between 3 and 5 hours ago
        # The end_times are: 0.5h ago, 1.5 ago, 2.5, 3.5, 4.5, 5.5, ...
        # So we should have 2 jobs land in this window
        end_after = (now - timedelta(hours=5)).isoformat() + "Z"
        end_before = (now - timedelta(hours=3)).isoformat() + "Z"
        jobs = self.client.get_data(
            "batchjob-list",
            end_time_after=end_after,
            end_time_before=end_before,
            check=status.HTTP_200_OK,
        )
        self.assertEqual(jobs["count"], 2)
        jobs = jobs["results"]
        for job in jobs:
            self.assertIn("good", job["filter_tags"])

    def test_json_tags_filter_list(self):
        site = self.create_site()
        for priority in [None, 1, 2, 3]:
            for system in ["H2O", "D2O", "HF"]:
                if priority:
                    tags = {"priority": priority, "system": system}
                else:
                    tags = {"system": system}
                self.create_batchjob(site, filter_tags=tags)

        jobs = self.client.get_data("batchjob-list")
        self.assertEqual(jobs["count"], 12)
        jobs = self.client.get_data("batchjob-list", filter_tags__priority__gt=1)
        self.assertEqual(jobs["count"], 6)
        jobs = self.client.get_data("batchjob-list", filter_tags__priority__lt=1)
        self.assertEqual(jobs["count"], 0)
        jobs = self.client.get_data("batchjob-list", filter_tags__priority=3)
        self.assertEqual(jobs["count"], 3)
        jobs = self.client.get_data("batchjob-list", filter_tags__priority__isnull=True)
        self.assertEqual(jobs["count"], 3)

        jobs = self.client.get_data("batchjob-list", filter_tags__system="D2O")
        self.assertEqual(jobs["count"], 4)

        jobs = self.client.get_data(
            "batchjob-list",
            filter_tags__system__icontains="F",
            filter_tags__priority__isnull=True,
        )
        self.assertEqual(jobs["count"], 1)

    def test_search_by_hostname(self):
        site1 = self.create_site(hostname="theta")
        site2 = self.create_site(hostname="cooley")
        for s in [site1, site2]:
            for num_nodes in [128, 256]:
                self.create_batchjob(site=s, num_nodes=num_nodes)
        jobs = self.client.get_data("batchjob-list", search="thet")
        self.assertEqual(jobs["count"], 2)
        self.assertEqual(jobs["results"][0]["site"], site1["pk"])
        self.assertEqual(jobs["results"][1]["site"], site1["pk"])

    def test_order_by_listings(self):
        # Create a shuffled list of batchjobs
        site = self.create_site(hostname="theta")
        states = 5 * ["finished"] + 5 * ["running"]
        deltas = [timedelta(hours=random.randint(-30, -1)) for i in range(10)]
        random.shuffle(states)
        now = datetime.utcnow()
        start_times = [now + delta for delta in deltas]

        for state, start in zip(states, start_times):
            j = self.create_batchjob(site)
            j.update(state=state, start_time=start)
            self.client.put_data(
                "batchjob-detail", uri={"pk": j["pk"]}, check=status.HTTP_200_OK, **j
            )

        # Order by state and descending start time
        jobs = self.client.get_data(
            "batchjob-list", ordering="state,-start_time", check=status.HTTP_200_OK
        )
        jobs = jobs["results"]
        self.assertEqual(len(jobs), 10)
        states = [j["state"] for j in jobs]
        stimes_finished = [isoparse(j["start_time"]) for j in jobs[:5]]
        stimes_running = [isoparse(j["start_time"]) for j in jobs[5:]]
        self.assertListEqual(states, sorted(states))
        self.assertListEqual(stimes_finished, sorted(stimes_finished, reverse=True))
        self.assertListEqual(stimes_running, sorted(stimes_running, reverse=True))

    def test_paginated_responses(self):
        site = self.create_site(hostname="theta")
        site = Site.objects.first()
        jobs = [
            BatchJob(site=site, num_nodes=1, wall_time_min=1, job_mode="mpi")
            for i in range(2000)
        ]
        BatchJob.objects.bulk_create(jobs)

        # default page size is 100
        jobs = self.client.get_data("batchjob-list")
        self.assertEqual(jobs["count"], 2000)
        self.assertEqual(len(jobs["results"]), 100)
        self.assertIn("limit", jobs["next"])
        self.assertEqual(None, jobs["previous"])  # no previous page

        # larger page and offset:
        jobs = self.client.get_data("batchjob-list", limit=800, offset=200)
        self.assertEqual(jobs["count"], 2000)
        self.assertEqual(len(jobs["results"]), 800)
        self.assertIn("limit", jobs["next"])
        self.assertIn("limit", jobs["previous"])

    def test_detail_view(self):
        site = self.create_site(hostname="theta")
        job = self.create_batchjob(site)
        self.client.get_data(
            "batchjob-detail", uri={"pk": job["pk"]}, check=status.HTTP_200_OK
        )

    def test_update_to_invalid_state(self):
        site = self.create_site(hostname="theta")
        job = self.create_batchjob(site)
        job.update(num_nodes=4096, state="invalid-state")
        self.client.put_data(
            "batchjob-detail",
            uri={"pk": job["pk"]},
            **job,
            check=status.HTTP_400_BAD_REQUEST,
        )

    def test_update_valid(self):
        site = self.create_site(hostname="theta")
        job = self.create_batchjob(site)
        job.update(
            status_message="Please submit to another queue", state="submit-failed"
        )
        self.client.put_data(
            "batchjob-detail", uri={"pk": job["pk"]}, **job, check=status.HTTP_200_OK
        )
        ret = self.client.get_data("batchjob-detail", uri={"pk": job["pk"]})
        self.assertDictEqual(ret, job)

    def test_partial_update(self):
        site = self.create_site(hostname="theta")
        job = self.create_batchjob(site)
        patch = dict(
            status_message="You dont have permission to submit to this queue",
            state="submit-failed",
        )
        patched_job = self.client.patch_data(
            "batchjob-detail", uri={"pk": job["pk"]}, **patch, check=status.HTTP_200_OK
        )
        expected_result = job.copy()
        expected_result.update(patch)
        self.assertDictEqual(patched_job, expected_result)

    def test_bulk_status_update_batch_jobs(self):
        theta = self.create_site(hostname="theta")
        cooley = self.create_site(hostname="cooley")
        for _ in range(10):
            self.create_batchjob(theta)
            self.create_batchjob(cooley)

        # scheduler agent receives 10 batchjobs; sends back bulk-state updates
        jobs = self.client.get_data("batchjob-list", site=cooley["pk"])
        self.assertEqual(jobs["count"], 10)
        jobs = jobs["results"]
        for job in jobs[:5]:
            job["state"] = "queued"
        for job in jobs[5:]:
            job["state"] = "running"
            job["start_time"] = datetime.utcnow() + timedelta(
                minutes=random.randint(-30, 0)
            )

        updates = [
            {k: job[k] for k in job if k in ["pk", "state", "start_time"]}
            for job in jobs
        ]
        result = self.client.bulk_patch_data(
            "batchjob-list", check=status.HTTP_200_OK, list_data=updates
        )

        for updated_job in result:
            pk = updated_job["pk"]
            expected_job = next(j for j in jobs if j["pk"] == pk)
            if expected_job["start_time"] is not None:
                expected_job["start_time"] = (
                    expected_job["start_time"].isoformat() + "Z"
                )
            self.assertDictEqual(updated_job, expected_job)

        jobs = self.client.get_data("batchjob-list", site=cooley["pk"], state="running")
        self.assertEqual(jobs["count"], 5)

    def test_update_batchjob_before_running(self):
        site = self.create_site(hostname="theta")
        pk = self.create_batchjob(site, filter_tags={"system": "H2O"})["pk"]

        # The Balsam site (agent) and user retrieve job at same time
        user_job = self.client.get_data("batchjob-detail", uri={"pk": pk})
        site_job = self.client.get_data("batchjob-detail", uri={"pk": pk})

        # first the Site submits the job and bulk-partial-updates as "queued"
        site_job["state"] = "queued"
        site_job["scheduler_id"] = 123
        self.client.bulk_patch_data(
            "batchjob-list",
            check=status.HTTP_200_OK,
            list_data=[{"pk": pk, "state": "queued", "scheduler_id": 123}],
        )

        # Meanwhile, another client clears-out filter_tags on their stale job
        # We need to be using PATCH and partial-updating, to reduce likelihood
        # of clobbering updates with stale data
        user_job["filter_tags"] = {}
        user_job = self.client.patch_data(
            "batchjob-detail", uri={"pk": pk}, filter_tags={}, check=status.HTTP_200_OK
        )

        self.assertEqual(user_job["filter_tags"], {})
        self.assertEqual(user_job["state"], "queued")
        self.assertEqual(user_job["scheduler_id"], 123)

    def test_cannot_update_batchjob_after_running(self):
        site = self.create_site(hostname="theta")
        pk = self.create_batchjob(site, num_nodes=7)["pk"]
        # The Balsam site (agent) and user retrieve job at same time
        self.client.get_data("batchjob-detail", uri={"pk": pk})
        self.client.get_data("batchjob-detail", uri={"pk": pk})

        # Site runs job first
        self.client.bulk_patch_data(
            "batchjob-list",
            check=status.HTTP_200_OK,
            list_data=[{"pk": pk, "state": "running", "scheduler_id": 123}],
        )

        # Client attempts to change num_nodes with stale record
        response = self.client.patch_data(
            "batchjob-detail",
            uri={"pk": pk},
            num_nodes=27,
            check=status.HTTP_400_BAD_REQUEST,
        )
        self.assertIn("cannot be updated", response[0])

    def test_revert_stale_batchjob_update(self):
        # A BatchJob is added to user's theta site.
        site = self.create_site(hostname="theta")
        pk = self.create_batchjob(site, num_nodes=7)["pk"]

        # The balsam site retrieves the new job
        site_job = self.client.get_data("batchjob-detail", uri={"pk": pk})
        self.assertEqual(site_job["state"], "pending-submission")

        # The site submits the job to the local queue

        # The site updates state as "queued". Now there is a scheduler_id.
        site_job = self.client.patch_data(
            "batchjob-detail",
            uri={"pk": pk},
            state="queued",
            scheduler_id=123,
            check=status.HTTP_200_OK,
        )

        # Time goes by..the job has started running

        # The Balsam site (agent) and web client retrieve BatchJob
        site_job = self.client.get_data("batchjob-detail", uri={"pk": pk})
        web_user_job = self.client.get_data("batchjob-detail", uri={"pk": pk})

        # Web Client doesnt know it's running. Updates num_nodes=27
        web_user_job = self.client.patch_data(
            "batchjob-detail", uri={"pk": pk}, num_nodes=27, check=status.HTTP_200_OK
        )
        self.assertEqual(web_user_job["num_nodes"], 27)

        # Balsam site does qstat: the job has started running on 7 nodes
        qstat = {"state": "running", "scheduler_id": 123, "num_nodes": 7}

        # The site's record is stale!
        self.assertEqual(site_job["num_nodes"], 7)
        self.assertEqual(BatchJob.objects.get(pk=pk).num_nodes, 27)

        # In order to mitigate these invalid updates on stale BatchJobs, the site
        # includes revert=True on all 'running' status updates
        site_job.update(**qstat, revert=True)
        self.client.bulk_patch_data(
            "batchjob-list", check=status.HTTP_200_OK, list_data=[site_job]
        )

        # The BatchJob record is now updated to running and all
        # fields forced to match qstat values
        web_user_job = self.client.get_data("batchjob-detail", uri={"pk": pk})
        site_job.pop("revert")
        self.assertDictEqual(web_user_job, site_job)
        self.assertEqual(site_job["num_nodes"], 7)
        self.assertEqual(site_job["state"], "running")

    def test_revert_does_not_override_deletion_state(self):
        site = self.create_site(hostname="theta")
        pk = self.create_batchjob(site, num_nodes=7)["pk"]

        self.client.get_data("batchjob-detail", uri={"pk": pk})
        site_job = self.client.get_data("batchjob-detail", uri={"pk": pk})

        # One client marks job for deletion
        self.client.patch_data(
            "batchjob-detail",
            uri={"pk": pk},
            check=status.HTTP_200_OK,
            state="pending-deletion",
        )

        # Site updates job for running (therefore revert=True to enforce consistency)
        site_job.update(scheduler_id=444, state="running", revert=True)
        site_job = self.client.bulk_patch_data(
            "batchjob-list", check=status.HTTP_200_OK, list_data=[site_job]
        )[0]

        # However, state "running" does not overwrite "pending-deletion"
        self.assertEqual(site_job["state"], "pending-deletion")
        self.assertEqual(site_job["scheduler_id"], 444)
        # Now the site knows to 'qdel' the job...
        site_job.update(state="finished")
        site_job = self.client.bulk_patch_data(
            "batchjob-list", check=status.HTTP_200_OK, list_data=[site_job]
        )[0]
        self.assertEqual(site_job["state"], "finished")

    def test_cannot_update_batchjob_after_terminal_state(self):
        site = self.create_site(hostname="theta")
        pk = self.create_batchjob(site, num_nodes=7)["pk"]
        user_job = self.client.get_data("batchjob-detail", uri={"pk": pk})
        site_job = self.client.get_data("batchjob-detail", uri={"pk": pk})

        site_job.update(state="finished")
        site_job = self.client.bulk_patch_data(
            "batchjob-list", check=status.HTTP_200_OK, list_data=[site_job]
        )[0]

        user_job = self.client.patch_data(
            "batchjob-detail",
            uri={"pk": pk},
            state="pending-deletion",
            check=status.HTTP_400_BAD_REQUEST,
        )
        self.assertIn("state can no longer change", user_job[0])

    def test_delete_running_batchjob(self):
        site = self.create_site(hostname="theta")
        pk = self.create_batchjob(site, num_nodes=7)["pk"]
        user_job = self.client.get_data("batchjob-detail", uri={"pk": pk})
        site_job = self.client.get_data("batchjob-detail", uri={"pk": pk})
        self.assertEqual(user_job["scheduler_id"], None)

        # site updates to running
        site_job.update(
            state="running", start_time=datetime.utcnow(), scheduler_id=123, revert=True
        )
        site_job = self.client.bulk_patch_data(
            "batchjob-list", check=status.HTTP_200_OK, list_data=[site_job]
        )[0]
        self.assertEqual(site_job["state"], "running")

        # user patches to pending-deletion
        user_job = self.client.patch_data(
            "batchjob-detail",
            uri={"pk": pk},
            check=status.HTTP_200_OK,
            state="pending-deletion",
        )
        self.assertEqual(user_job["state"], "pending-deletion")
        self.assertEqual(user_job["scheduler_id"], 123)

        # Client receives job marked for deletion
        site_job = self.client.get_data("batchjob-detail", uri={"pk": pk})
        self.assertEqual(site_job["state"], "pending-deletion")
        site_job.update(
            state="finished", status_message="user-deleted", end_time=datetime.utcnow()
        )
        patch = {k: site_job[k] for k in ["pk", "state", "status_message", "end_time"]}
        site_job = self.client.bulk_patch_data(
            "batchjob-list", check=status.HTTP_200_OK, list_data=[patch]
        )[0]
        self.assertEqual(site_job["state"], "finished")


class BatchJobPrivacyTests(TwoUserTestCase, SiteFactoryMixin, BatchJobFactoryMixin):
    def test_no_shared_batchjobs_in_list_view(self):
        """client2 cannot see client1's batchjobs"""
        site1 = self.create_site(hostname="site1", client=self.client1)
        self.create_site(hostname="site2", client=self.client2)
        self.assertEqual(Site.objects.get(hostname="site1").owner_id, self.user1.pk)
        self.assertEqual(Site.objects.get(hostname="site2").owner_id, self.user2.pk)

        # client1 adds batchjob to site1
        self.create_batchjob(site1, client=self.client1)

        # client2 cannot see it
        jobs = self.client2.get_data("batchjob-list", check=status.HTTP_200_OK)
        self.assertEqual(jobs["count"], 0)

    def test_permission_in_detail_view(self):
        """client2 cannot see client1's batchjobs in detail view"""
        site1 = self.create_site(hostname="site1", client=self.client1)
        self.create_site(hostname="site2", client=self.client2)

        # client1 adds batchjob to site1
        pk = self.create_batchjob(site1, client=self.client1)["pk"]

        # client2 gets 404 not found, but client1 can see it
        self.client2.get_data(
            "batchjob-detail", {"pk": pk}, check=status.HTTP_404_NOT_FOUND
        )
        self.client1.get_data("batchjob-detail", {"pk": pk}, check=status.HTTP_200_OK)

    def test_bulk_update_cannot_affect_other_users_batchjobs(self):
        """client2 bulk-update cannot affect client1's batchjobs"""
        # a batchjob added to site1 belonging to user1
        site1 = self.create_site(hostname="site1", client=self.client1)
        pk = self.create_batchjob(site1, client=self.client1)["pk"]

        # client 2 attempts bulk update with client1's batchjob id; fails
        patch = {"pk": pk, "state": "pending-deletion"}
        self.client2.bulk_patch_data(
            "batchjob-list", check=status.HTTP_400_BAD_REQUEST, list_data=[patch]
        )

        # client 1 can do it, though:
        self.client1.bulk_patch_data(
            "batchjob-list", check=status.HTTP_200_OK, list_data=[patch]
        )
