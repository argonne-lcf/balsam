import random
from datetime import datetime, timedelta

from fastapi import status

from .util import create_site


def test_can_create_batchjob(auth_client):
    site1 = create_site(auth_client, hostname="site1")
    batch_job = auth_client.post(
        "/batch-jobs/",
        site_id=site1["id"],
        project="datascience",
        queue="default",
        num_nodes=4,
        wall_time_min=60,
        job_mode="mpi",
        optional_params={},
        filter_tags={},
    )
    assert batch_job["state"] == "pending_submission"
    assert batch_job["scheduler_id"] is None


def test_list_batchjobs_spanning_sites(auth_client):
    site1 = create_site(auth_client, hostname="site1")
    site2 = create_site(auth_client, hostname="site2")
    for time in [10, 20, 30, 40]:
        for site in [site1, site2]:
            auth_client.post(
                "/batch-jobs/",
                site_id=site["id"],
                project="datascience",
                queue="default",
                num_nodes=4,
                wall_time_min=time,
                job_mode="mpi",
                optional_params={},
                filter_tags={},
            )
    bjob_list = auth_client.get("/batch-jobs")
    assert bjob_list["count"] == 8


def test_filter_by_site(auth_client):
    site1 = create_site(auth_client, hostname="site1")
    site2 = create_site(auth_client, hostname="site2")
    for time in [10, 20, 30, 40]:
        for site in [site1, site2]:
            auth_client.post(
                "/batch-jobs/",
                site_id=site["id"],
                project="datascience",
                queue="default",
                num_nodes=4,
                wall_time_min=time,
                job_mode="mpi",
                optional_params={},
                filter_tags={},
            )
    # providing GET kwargs causes result list to be filtered
    bjob_list = auth_client.get("/batch-jobs", site_id=site2["id"])
    assert bjob_list["count"] == 4
    for bjob in bjob_list["results"]:
        assert bjob["site_id"] == site2["id"]


def test_filter_by_time_range(auth_client):
    site = create_site(auth_client, hostname="site1")
    # Create 10 historical batchjobs
    # Job n started n hours ago and took 30 minutes
    now = datetime.utcnow()
    for i in range(1, 11):
        start = now - timedelta(hours=i * 1)
        end = start + timedelta(minutes=30)
        if now - timedelta(hours=5) <= end <= now - timedelta(hours=3):
            filter_tags = {"good": "Yes"}
        else:
            filter_tags = {}
        bjob = auth_client.post(
            "/batch-jobs/",
            site_id=site["id"],
            project="datascience",
            queue="default",
            num_nodes=4,
            wall_time_min=30,
            job_mode="mpi",
            filter_tags=filter_tags,
        )
        auth_client.put(
            f"/batch-jobs/{bjob['id']}",
            state="finished",
            start_time=start,
            end_time=end,
        )

    # Now, we want to filter for jobs that ended between 3 and 5 hours ago
    # The end_times are: 0.5h ago, 1.5 ago, 2.5, 3.5, 4.5, 5.5, ...
    # So we should have 2 jobs land in this window
    end_after = now - timedelta(hours=5)
    end_before = now - timedelta(hours=3)
    jobs = auth_client.get("/batch-jobs/", end_time_after=end_after, end_time_before=end_before)
    assert jobs["count"] == 2 == len(jobs["results"])
    for job in jobs["results"]:
        assert "good" in job["filter_tags"]


def test_json_tags_filter_list(auth_client):
    site = create_site(auth_client, hostname="site1")
    for priority in [None, 1, 2, 3]:
        for system in ["H2O", "D2O", "HF"]:
            if priority:
                tags = {"priority": priority, "system": system}
            else:
                tags = {"system": system}
            auth_client.post(
                "/batch-jobs/",
                site_id=site["id"],
                project="datascience",
                queue="default",
                num_nodes=4,
                wall_time_min=30,
                job_mode="mpi",
                filter_tags=tags,
            )

    jobs = auth_client.get("/batch-jobs/")
    assert jobs["count"] == 12 == len(jobs["results"])

    jobs = auth_client.get("/batch-jobs/", filter_tags="priority:2")
    assert jobs["count"] == 3 == len(jobs["results"])

    jobs = auth_client.get("/batch-jobs/", filter_tags="system:D2O")
    assert jobs["count"] == 4 == len(jobs["results"])

    jobs = auth_client.get("/batch-jobs/", filter_tags=["system:D2O", "priority:1"])
    assert jobs["count"] == 1 == len(jobs["results"])


def test_detail_view(auth_client):
    site = create_site(auth_client, hostname="site1")
    bjob = auth_client.post(
        "/batch-jobs/",
        site_id=site["id"],
        project="datascience",
        queue="default",
        num_nodes=4,
        wall_time_min=30,
        job_mode="mpi",
    )
    id = bjob["id"]
    retrieved = auth_client.get(f"/batch-jobs/{id}")
    assert retrieved == bjob


def test_update_to_invalid_state(auth_client):
    site = create_site(auth_client, hostname="site1")
    bjob = auth_client.post(
        "/batch-jobs/",
        site_id=site["id"],
        project="datascience",
        queue="default",
        num_nodes=4,
        wall_time_min=30,
        job_mode="mpi",
    )
    id = bjob["id"]
    auth_client.put(
        f"batch-jobs/{id}",
        state="invalid-state",
        check=status.HTTP_422_UNPROCESSABLE_ENTITY,
    )


def test_update_valid(auth_client):
    site = create_site(auth_client, hostname="site1")
    bjob = auth_client.post(
        "/batch-jobs/",
        site_id=site["id"],
        project="datascience",
        queue="default",
        num_nodes=4,
        wall_time_min=30,
        job_mode="mpi",
    )
    id = bjob["id"]
    auth_client.put(
        f"batch-jobs/{id}",
        state="submit_failed",
        status_info={"error": "User is not a member of project datascience"},
    )
    ret = auth_client.get(f"/batch-jobs/{id}")
    assert ret["state"] == "submit_failed"
    assert "error" in ret["status_info"]


def test_bulk_status_update_batch_jobs(auth_client):
    theta = create_site(auth_client, hostname="theta")
    cooley = create_site(auth_client, hostname="cooley")
    for _ in range(10):
        auth_client.post(
            "/batch-jobs/",
            site_id=theta["id"],
            project="datascience",
            queue="default",
            num_nodes=4,
            wall_time_min=30,
            job_mode="mpi",
        )
        auth_client.post(
            "/batch-jobs/",
            site_id=cooley["id"],
            project="datascience",
            queue="default",
            num_nodes=4,
            wall_time_min=30,
            job_mode="mpi",
        )

    # scheduler agent receives 10 batchjobs; sends back bulk-state updates
    jobs = auth_client.get("/batch-jobs/", site_id=cooley["id"])
    assert jobs["count"] == 10
    jobs = jobs["results"]
    for job in jobs[:5]:
        job["state"] = "queued"
    for job in jobs[5:]:
        job["state"] = "running"
        job["start_time"] = datetime.utcnow() + timedelta(minutes=random.randint(-30, 0))

    updates = [{k: job[k] for k in job if k in ["id", "state", "start_time"]} for job in jobs]
    result = auth_client.bulk_patch("/batch-jobs/", updates)

    for updated_job in result:
        id = updated_job["id"]
        expected_state = next(j["state"] for j in jobs if j["id"] == id)
        assert updated_job["state"] == expected_state

    jobs = auth_client.get("/batch-jobs/", site_id=cooley["id"], state="running")
    assert jobs["count"] == len(jobs["results"]) == 5


def test_delete_running_batchjob(auth_client):
    site = create_site(auth_client, hostname="theta")
    bjob = auth_client.post(
        "/batch-jobs/",
        site_id=site["id"],
        project="datascience",
        queue="default",
        num_nodes=7,
        wall_time_min=30,
        job_mode="mpi",
    )
    id = bjob["id"]
    user_job = auth_client.get(f"/batch-jobs/{id}")
    site_job = auth_client.get(f"/batch-jobs/{id}")

    # site updates to running
    site_job = auth_client.put(
        f"/batch-jobs/{id}",
        scheduler_id=123,
        start_time=datetime.utcnow(),
        state="running",
    )
    assert site_job["state"] == "running"
    assert site_job["scheduler_id"] == 123

    # user patches to pending_deletion
    user_job = auth_client.put(f"/batch-jobs/{id}", state="pending_deletion")
    assert user_job["state"] == "pending_deletion"

    # Client receives job marked for deletion
    site_job = auth_client.get(f"/batch-jobs/{id}")
    assert site_job["state"] == "pending_deletion"
    patch = {
        "id": id,
        "state": "finished",
        "end_time": datetime.utcnow(),
        "status_info": {"message": "User deleted job"},
    }
    auth_client.bulk_patch("/batch-jobs/", [patch])

    # User sees the deletion eventually
    user_job = auth_client.get(f"/batch-jobs/{id}")
    assert user_job["state"] == "finished"
    assert "User deleted job" in user_job["status_info"].values()


def test_delete_api_endpoint(auth_client):
    site = create_site(auth_client, hostname="theta")
    bjob = auth_client.post(
        "/batch-jobs/",
        site_id=site["id"],
        project="datascience",
        queue="default",
        num_nodes=7,
        wall_time_min=30,
        job_mode="mpi",
    )
    all = auth_client.get("/batch-jobs/")
    assert all["count"] == 1
    resp = auth_client.delete(f"/batch-jobs/{bjob['id']}")
    print("delete response is", resp)
    all = auth_client.get("/batch-jobs/")
    assert all["count"] == 0


def test_no_shared_batchjobs_in_list_view(fastapi_user_test_client):
    """client2 cannot see client1's batchjobs"""
    client1, client2 = fastapi_user_test_client(), fastapi_user_test_client()
    site1 = create_site(client1, path="/bar")
    create_site(client2, path="/foo")

    # client1 adds batchjob to site1
    client1.post(
        "/batch-jobs/",
        site_id=site1["id"],
        project="datascience",
        queue="default",
        num_nodes=7,
        wall_time_min=30,
        job_mode="mpi",
    )
    # client2 cannot see it
    assert client2.get("/batch-jobs")["count"] == 0
    assert client1.get("/batch-jobs")["count"] == 1


def test_permission_in_detail_view(fastapi_user_test_client):
    """client2 cannot see client1's batchjobs in detail view"""
    client1, client2 = fastapi_user_test_client(), fastapi_user_test_client()
    site1 = create_site(client1, path="/bar")
    create_site(client2, path="/foo")
    bjob = client1.post(
        "/batch-jobs/",
        site_id=site1["id"],
        project="datascience",
        queue="default",
        num_nodes=7,
        wall_time_min=30,
        job_mode="mpi",
    )
    id = bjob["id"]
    # client2 gets 404 if try to access client1's bjob
    client2.get(f"/batch-jobs/{id}", check=status.HTTP_404_NOT_FOUND)
    client1.get(f"/batch-jobs/{id}", check=status.HTTP_200_OK)


def test_bulk_update_cannot_affect_other_users_batchjobs(fastapi_user_test_client):
    """client2 bulk-update cannot affect client1's batchjobs"""
    client1, client2 = fastapi_user_test_client(), fastapi_user_test_client()
    site1 = create_site(client1, path="/bar")
    create_site(client2, path="/foo")
    bjob = client1.post(
        "/batch-jobs/",
        site_id=site1["id"],
        project="datascience",
        queue="default",
        num_nodes=7,
        wall_time_min=30,
        job_mode="mpi",
    )

    # client 2 attempts bulk update with client1's batchjob id; fails
    patch = {"id": bjob["id"], "state": "pending_deletion"}
    client2.bulk_patch("/batch-jobs/", [patch], check=status.HTTP_400_BAD_REQUEST)

    # client 1 can do it, though:
    client1.bulk_patch("/batch-jobs/", [patch], check=status.HTTP_200_OK)
