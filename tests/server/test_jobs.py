"""APIClient-driven tests"""
import random
import time
from datetime import datetime, timedelta
from uuid import uuid4

import pytest
from dateutil.parser import isoparse
from fastapi import status

from balsam.server import models

from .util import create_app, create_site

FAST_EXPIRATION_PERIOD = 0.6


@pytest.fixture(scope="function")
def site(auth_client):
    """Called before each test"""
    return create_site(
        auth_client,
        hostname="site1",
        transfer_locations={"MyCluster": f"globus://{uuid4()}"},
    )


@pytest.fixture(scope="function")
def app(auth_client, site):
    return create_app(auth_client, site_id=site["id"], class_path="DemoApp.hello")


@pytest.fixture(scope="function")
def job_dict(app):
    def _job_dict(
        app_id=None,
        workdir="test/1",
        tags={},
        transfers={"hello-input": {"location_alias": "MyCluster", "path": "/path/to/input.dat"}},
        parameters={"name": "world", "N": 4},
        data={},
        return_code=None,
        parent_ids=[],
        num_nodes=2,
        ranks_per_node=4,
        threads_per_rank=1,
        threads_per_core=1,
        launch_params={},
        gpus_per_rank=0,
        node_packing_count=1,
        wall_time_min=0,
    ):
        if app_id is None:
            app_id = app["id"]
        return dict(
            app_id=app_id,
            workdir=workdir,
            tags=tags,
            transfers=transfers,
            parameters=parameters,
            data=data,
            return_code=return_code,
            parent_ids=parent_ids,
            num_nodes=num_nodes,
            ranks_per_node=ranks_per_node,
            threads_per_rank=threads_per_rank,
            threads_per_core=threads_per_core,
            launch_params=launch_params,
            gpus_per_rank=gpus_per_rank,
            node_packing_count=node_packing_count,
            wall_time_min=wall_time_min,
        )

    return _job_dict


@pytest.fixture(scope="function")
def create_session(job_dict, site, db_session):
    def _create_session():
        bjob = models.BatchJob(
            site_id=site["id"],
            scheduler_id=123,
            project="foo",
            queue="default",
            state="running",
            num_nodes=32,
            wall_time_min=60,
            job_mode="mpi",
            filter_tags={},
        )
        db_session.add(bjob)
        session = models.Session(site_id=bjob.site_id, batch_job=bjob)
        db_session.commit()
        return session

    return _create_session


@pytest.fixture(scope="function")
def fast_session_expiry():
    old_expiry = models.crud.sessions.SESSION_EXPIRE_PERIOD
    old_sweep = models.crud.sessions.SESSION_SWEEP_PERIOD
    try:
        models.crud.sessions.SESSION_EXPIRE_PERIOD = timedelta(seconds=FAST_EXPIRATION_PERIOD)
        models.crud.sessions.SESSION_SWEEP_PERIOD = timedelta(seconds=0.1)
        yield
    finally:
        models.crud.sessions.SESSION_EXPIRE_PERIOD = old_expiry
        models.crud.sessions.SESSION_SWEEP_PERIOD = old_sweep


def assertHistory(client, job, *states, **expected_messages):
    """
    Assert that `job` went through the sequence of `states` in order.
    For each state:str pair in `expected_messages`, verify the str is contained
    in the transition log message.
    """
    response = client.get("/events", job_id=job["id"])
    fail_msg = "\n" + "\n".join(
        f'{i}) {e["from_state"]} ->  {e["to_state"]} ({e["data"]})' for i, e in enumerate(response["results"])
    )
    assert response["count"] == len(states) - 1, fail_msg
    eventlogs = list(reversed(response["results"]))

    for i, (from_state, to_state) in enumerate(zip(states[:-1], states[1:])):
        expected_dict = {"from_state": from_state, "to_state": to_state}
        event = eventlogs[i]
        actual = {key: event[key] for key in ("from_state", "to_state")}
        assert expected_dict == actual
        if to_state in expected_messages:
            assert expected_messages.pop(to_state) in event["data"]["message"]


@pytest.fixture(scope="function")
def linear_dag(auth_client, job_dict):
    A = auth_client.bulk_post("/jobs/", [job_dict(tags={"step": "A", "dag": "dag1"})])[0]
    B = auth_client.bulk_post("/jobs/", [job_dict(tags={"step": "B", "dag": "dag1"}, parent_ids=[A["id"]])])[0]
    C = auth_client.bulk_post("/jobs/", [job_dict(tags={"step": "C", "dag": "dag1"}, parent_ids=[B["id"]])])[0]
    return A, B, C


def test_add_jobs(auth_client, job_dict):
    jobs = [
        job_dict(
            parameters={"name": "foo", "N": i},
            workdir=f"test/{i}",
        )
        for i in range(3)
    ]
    jobs = auth_client.bulk_post("/jobs/", jobs)
    for job in jobs:
        assert job["state"] == "READY"
        assertHistory(auth_client, job, "CREATED", "READY")


def test_bad_job_parameters_refused(auth_client, job_dict):
    jobs = [job_dict(parameters={})]
    response = auth_client.bulk_post("/jobs/", jobs, check=status.HTTP_400_BAD_REQUEST)
    assert "missing parameters" in str(response)

    jobs = [job_dict(parameters={"name": "foo", "name2": "bar"})]
    response = auth_client.bulk_post("/jobs/", jobs, check=status.HTTP_400_BAD_REQUEST)
    assert "extraneous parameters" in str(response)


def test_child_with_two_parents_state_update(auth_client, job_dict):
    resp = auth_client.bulk_post("/jobs/", [job_dict()])
    parent1 = resp[0]
    resp = auth_client.bulk_post("/jobs/", [job_dict()])
    parent2 = resp[0]
    resp = auth_client.bulk_post("/jobs/", [job_dict(parent_ids=[parent1["id"], parent2["id"]])])
    child = resp[0]
    assert child["state"] == "AWAITING_PARENTS"

    auth_client.bulk_patch("/jobs/", [{"id": parent1["id"], "state": "JOB_FINISHED"}])
    child = auth_client.get(f"/jobs/{child['id']}")
    assert child["state"] == "AWAITING_PARENTS"

    auth_client.bulk_patch("/jobs/", [{"id": parent2["id"], "state": "JOB_FINISHED"}])
    child = auth_client.get(f"/jobs/{child['id']}")
    assert child["state"] == "READY"


def test_parent_with_two_children_state_update(auth_client, job_dict):
    resp = auth_client.bulk_post("/jobs/", [job_dict()])
    parent = resp[0]
    resp = auth_client.bulk_post("/jobs/", [job_dict(parent_ids=[parent["id"]])])
    child1 = resp[0]
    resp = auth_client.bulk_post("/jobs/", [job_dict(parent_ids=[parent["id"]])])
    child2 = resp[0]

    assert parent["state"] == "READY"
    assert child1["state"] == "AWAITING_PARENTS" == child2["state"]

    # POSTPROCESSED Job cascades to JOB_FINISHED:
    auth_client.bulk_put("/jobs/", {"state": "POSTPROCESSED"}, id=parent["id"])
    child1 = auth_client.get(f"/jobs/{child1['id']}")
    child2 = auth_client.get(f"/jobs/{child2['id']}")
    assert child1["state"] == "READY"
    assert child2["state"] == "READY"


def test_add_job_without_transfers_is_STAGED_IN(auth_client, job_dict):
    """Ready and bound to backend"""
    job = auth_client.bulk_post("/jobs/", [job_dict(transfers=[])])[0]
    assert job["state"] == "STAGED_IN"


def test_bulk_put(auth_client, job_dict):
    jobs = auth_client.bulk_post("/jobs/", [job_dict(transfers=[]) for _ in range(10)])
    for job in jobs:
        assert job["state"] == "STAGED_IN"
    ids = [j["id"] for j in jobs]
    jobs = auth_client.bulk_put("/jobs/", {"state": "PREPROCESSED"}, id=ids)
    for job in jobs:
        assert job["state"] == "PREPROCESSED"


def test_bulk_patch(auth_client, job_dict):
    jobs = auth_client.bulk_post("/jobs/", [job_dict(transfers=[]) for _ in range(10)])
    for job in jobs:
        assert job["state"] == "STAGED_IN"
    ids = [j["id"] for j in jobs]
    jobs = auth_client.bulk_patch("/jobs/", [{"id": id, "state": "PREPROCESSED"} for id in ids])
    for job in jobs:
        assert job["state"] == "PREPROCESSED"


def test_acquire_for_launch(auth_client, job_dict, create_session):
    """Jobs become associated with BatchJob"""
    jobs = auth_client.bulk_post("/jobs/", [job_dict(transfers=[]) for _ in range(10)])
    for job in jobs:
        assert job["state"] == "STAGED_IN"
        assert job["batch_job_id"] is None

    # Mark jobs PREPROCESSED and ready to run
    ids = [j["id"] for j in jobs]
    app_ids = {jobs[0]["app_id"]}
    auth_client.bulk_put("/jobs/", {"state": "PREPROCESSED"}, id=ids)

    session1, session2 = create_session(), create_session()

    # Launcher1 acquires 2 runnable jobs:
    acquired1 = auth_client.post(
        f"/sessions/{session1.id}",
        max_wall_time_min=120,
        filter_tags={},
        max_num_jobs=2,
        max_nodes_per_job=32,
        app_ids=app_ids,
        check=status.HTTP_200_OK,
    )

    # Launcher2 asks for up to 1000 and gets all the rest:
    acquired2 = auth_client.post(
        f"/sessions/{session2.id}",
        max_wall_time_min=120,
        filter_tags={},
        max_num_jobs=1000,
        max_nodes_per_job=32,
        app_ids=app_ids,
        check=status.HTTP_200_OK,
    )

    # These jobs are now associated with the corresponding BatchJob
    assert len(acquired1) == 2
    assert len(acquired2) == 8
    for job in acquired1:
        assert job["batch_job_id"] == session1.batch_job_id
    for job in acquired2:
        assert job["batch_job_id"] == session2.batch_job_id


def test_update_to_running_does_not_release_lock(auth_client, job_dict, create_session, db_session):
    jobs = auth_client.bulk_post("/jobs/", [job_dict(transfers=[]) for _ in range(10)])

    # Mark jobs PREPROCESSED
    ids = [j["id"] for j in jobs]
    app_ids = {jobs[0]["app_id"]}
    auth_client.bulk_put("/jobs/", {"state": "PREPROCESSED"}, id=ids)

    session = create_session()
    # Launcher1 acquires all of them
    acquired = auth_client.post(
        f"/sessions/{session.id}",
        max_wall_time_min=120,
        filter_tags={},
        max_num_jobs=100,
        max_nodes_per_job=32,
        app_ids=app_ids,
        check=status.HTTP_200_OK,
    )

    # Behind the scenes, the acquired jobs are locked:
    for job in db_session.query(models.Job).all():
        assert job.session_id == session.id

    # The jobs start running in a staggered fashion; a bulk status update is made
    run_start_times = [datetime.utcnow() + timedelta(seconds=random.randint(0, 20)) for _ in acquired]
    updates = [
        {
            "id": j["id"],
            "state": "RUNNING",
            "state_timestamp": ts,
            "state_message": "Running on Theta nid00139",
        }
        for j, ts in zip(acquired, run_start_times)
    ]
    jobs = auth_client.bulk_patch("/jobs/", updates)

    # The jobs are associated to batchjob and have the expected history:
    for job in jobs:
        assert job["batch_job_id"] == session.batch_job_id
        assertHistory(auth_client, job, "CREATED", "STAGED_IN", "PREPROCESSED", "RUNNING")

    # Behind the scenes, the acquired jobs have changed state and are still locked:
    for job in db_session.query(models.Job).all():
        assert job.session_id == session.id

    # The EventLogs were correctly recorded:
    time_stamps_in_db = (
        db_session.query(models.LogEvent.timestamp).filter(models.LogEvent.to_state == "RUNNING").all()
    )
    time_stamps_in_db = set(tup[0] for tup in time_stamps_in_db)
    assert time_stamps_in_db == set(run_start_times)


def test_acquire_for_launch_with_node_constraints(auth_client, job_dict, create_session):
    jobs = [
        *[job_dict(num_nodes=1, ranks_per_node=1, wall_time_min=40) for _ in range(2)],
        *[job_dict(num_nodes=1, ranks_per_node=1, wall_time_min=30) for _ in range(2)],
        *[job_dict(num_nodes=1, ranks_per_node=1, node_packing_count=4, wall_time_min=30) for _ in range(4)],
        *[job_dict(num_nodes=1, ranks_per_node=1, wall_time_min=50) for _ in range(2)],
        *[job_dict(num_nodes=1, ranks_per_node=4, wall_time_min=30) for _ in range(2)],
        *[job_dict(num_nodes=3, wall_time_min=0) for _ in range(2)],
        *[job_dict(num_nodes=8, wall_time_min=0) for _ in range(2)],
        *[job_dict(num_nodes=16, wall_time_min=0) for _ in range(2)],
    ]
    jobs = auth_client.bulk_post("/jobs/", jobs)
    auth_client.bulk_put("/jobs/", {"state": "PREPROCESSED"}, id=[j["id"] for j in jobs])
    session = create_session()

    acquired = auth_client.post(
        f"/sessions/{session.id}",
        max_wall_time_min=40,
        filter_tags={},
        max_num_jobs=32,
        max_nodes_per_job=1,
        serial_only=True,
        check=status.HTTP_200_OK,
    )
    assert len(acquired) == 8
    assert all(job["num_nodes"] == 1 for job in acquired)
    assert all(job["ranks_per_node"] == 1 for job in acquired)
    assert acquired == sorted(acquired, key=lambda job: (job["node_packing_count"], -1 * job["wall_time_min"]))

    acquired = auth_client.post(
        f"/sessions/{session.id}",
        max_wall_time_min=40,
        filter_tags={},
        max_num_jobs=100,
        max_nodes_per_job=8,
        check=status.HTTP_200_OK,
    )
    assert len(acquired) == 6
    assert all(1 <= job["num_nodes"] <= 8 for job in acquired)


def test_acquire_by_tags(auth_client, job_dict, create_session):
    jobs = [
        *[job_dict(tags={"system": "H2O", "calc": "energy"}) for _ in range(3)],
        *[job_dict(tags={"system": "H2O", "calc": "vib"}) for _ in range(3)],
        *[job_dict(tags={"system": "D2O", "calc": "energy"}) for _ in range(3)],
    ]
    jobs = auth_client.bulk_post("/jobs/", jobs)
    auth_client.bulk_put("/jobs/", {"state": "PREPROCESSED"}, id=[j["id"] for j in jobs])
    session = create_session()

    acquired = auth_client.post(
        f"/sessions/{session.id}",
        max_wall_time_min=40,
        filter_tags={"system": "T2O"},
        max_nodes_per_job=8,
        max_num_jobs=8,
        check=status.HTTP_200_OK,
    )
    assert len(acquired) == 0

    acquired = auth_client.post(
        f"/sessions/{session.id}",
        max_wall_time_min=40,
        filter_tags={"system": "H2O", "calc": "vib"},
        max_nodes_per_job=8,
        max_num_jobs=8,
        check=status.HTTP_200_OK,
    )
    assert len(acquired) == 3
    assert all(job["tags"]["system"] == "H2O" for job in acquired)
    assert all(job["tags"]["calc"] == "vib" for job in acquired)

    acquired = auth_client.post(
        f"/sessions/{session.id}",
        max_wall_time_min=40,
        filter_tags={"calc": "energy"},
        max_nodes_per_job=8,
        max_num_jobs=8,
        check=status.HTTP_200_OK,
    )
    assert len(acquired) == 6
    assert all(job["tags"]["calc"] == "energy" for job in acquired)


def test_bulk_update_based_on_tags_filter_via_put(auth_client, job_dict):
    specs = [
        job_dict(tags={"mass": 1.0}, num_nodes=1),
        job_dict(tags={"mass": 1.0}, num_nodes=1),
        job_dict(tags={"mass": 2.0}, num_nodes=1),
        job_dict(tags={"mass": 2.0}, num_nodes=1),
    ]
    auth_client.bulk_post("/jobs/", specs)
    auth_client.bulk_put(
        "/jobs/",
        {"num_nodes": 128},
        tags=["mass:2.0"],
    )
    jobs = auth_client.get("/jobs/")["results"]
    assert len(jobs) == 4
    for j in jobs:
        if j["tags"]["mass"] == "1.0":
            assert j["num_nodes"] == 1
        else:
            assert j["tags"]["mass"] == "2.0"
            assert j["num_nodes"] == 128


def test_can_filter_on_id(auth_client, job_dict):
    specs = [
        job_dict(workdir="A"),
        job_dict(workdir="B"),
        job_dict(workdir="C"),
    ]
    A, B, C = auth_client.bulk_post("/jobs/", specs)
    res = auth_client.get("/jobs/", id=[B["id"], C["id"]], ordering="workdir")
    assert res["count"] == 2
    workdirs = [job["workdir"] for job in res["results"]]
    assert workdirs == ["B", "C"]


def test_can_filter_on_parents(auth_client, job_dict):
    specs = [job_dict(workdir="A"), job_dict(workdir="B")]
    parentA, parentB = auth_client.bulk_post("/jobs/", specs)

    child_specs = [
        job_dict(workdir="A1", parent_ids=[parentA["id"]]),
        job_dict(workdir="A2", parent_ids=[parentA["id"]]),
        job_dict(workdir="B1", parent_ids=[parentB["id"]]),
        job_dict(workdir="B2", parent_ids=[parentB["id"]]),
        job_dict(workdir="B3", parent_ids=[parentB["id"]]),
        job_dict(workdir="C1", parent_ids=[parentA["id"], parentB["id"]]),
    ]
    auth_client.bulk_post("/jobs/", child_specs)

    children_of_B = auth_client.get(
        "/jobs/",
        parent_id=[parentB["id"]],
        ordering="workdir",
    )
    assert children_of_B["count"] == 4
    workdirs = [job["workdir"] for job in children_of_B["results"]]
    assert workdirs == ["B1", "B2", "B3", "C1"]

    children = auth_client.get(
        "/jobs/",
        parent_id=[parentA["id"], parentB["id"]],
        ordering="workdir",
    )
    assert children["count"] == 6
    workdirs = [job["workdir"] for job in children["results"]]
    assert workdirs == ["A1", "A2", "B1", "B2", "B3", "C1"]


def test_can_filter_on_last_update(auth_client, job_dict):
    specs = [job_dict(workdir="A"), job_dict(workdir="B")]
    A, B = auth_client.bulk_post("/jobs/", specs)
    creation_time = datetime.utcnow()  # IMPORTANT! all times in UTC

    time.sleep(0.1)
    auth_client.bulk_patch("/jobs/", [{"id": B["id"], "state": "PREPROCESSED"}])

    # Before the creation_timestamp: only A
    jobs = auth_client.get("/jobs/", last_update_before=creation_time)
    assert jobs["count"] == 1
    assert jobs["results"][0]["workdir"] == "A"

    # After the creation_timestamp: only B
    jobs = auth_client.get("/jobs/", last_update_after=creation_time)
    assert jobs["count"] == 1
    assert jobs["results"][0]["workdir"] == "B"


def test_update_to_run_done_releases_lock_but_not_batch_job(auth_client, job_dict, create_session, db_session):
    auth_client.bulk_post("/jobs/", [job_dict() for _ in range(5)])
    auth_client.bulk_put("/jobs/", {"state": "PREPROCESSED"})

    # Before acqusition: no locks, no batchjobs assigned
    for job in db_session.query(models.Job):
        assert job.session_id is None
        assert job.batch_job_id is None

    # Acquisition
    session = create_session()
    acquired = auth_client.post(
        f"/sessions/{session.id}",
        max_wall_time_min=40,
        filter_tags={},
        max_nodes_per_job=8,
        max_num_jobs=8,
        check=status.HTTP_200_OK,
    )
    assert len(acquired) == 5

    # After acqusition: locks & batchjob assigned
    for job in db_session.query(models.Job):
        assert job.session_id == session.id
        assert job.batch_job_id == session.batch_job_id

    # Update all to RUNNING
    auth_client.bulk_put("/jobs/", {"state": "RUNNING"})

    # After RUNNING: locks & batchjob assigned
    db_session.expire_all()
    for job in db_session.query(models.Job):
        assert job.state == "RUNNING"
        assert job.session_id == session.id
        assert job.batch_job_id == session.batch_job_id

    # Update to RUN_DONE
    auth_client.bulk_put("/jobs/", {"state": "RUN_DONE"})

    # After RUN_DONE: locks freed; batchjob remains
    db_session.expire_all()
    for job in db_session.query(models.Job):
        assert job.state == "RUN_DONE"
        assert job.session_id is None
        assert job.batch_job_id == session.batch_job_id


def test_tick_heartbeat_extends_expiration(auth_client, job_dict, create_session, db_session):
    session = create_session()
    auth_client.bulk_post("/jobs/", [job_dict() for _ in range(5)])
    auth_client.bulk_put("/jobs/", {"state": "PREPROCESSED"})

    before_acquire = datetime.utcnow()
    for job in db_session.query(models.Job):
        assert job.session_id is None
    db_session.expire_all()

    # Acquire session lock on all jobs
    auth_client.post(
        f"/sessions/{session.id}",
        max_wall_time_min=40,
        filter_tags={},
        max_nodes_per_job=8,
        max_num_jobs=8,
        check=status.HTTP_200_OK,
    )

    db_session.refresh(session)
    after_acquire = session.heartbeat
    assert session.jobs.count() == 5
    assert before_acquire < after_acquire

    # Tick lock some time later
    time.sleep(0.15)
    auth_client.put(f"/sessions/{session.id}")

    db_session.refresh(session)
    after_tick = session.heartbeat
    assert (after_tick - after_acquire) > timedelta(seconds=0.1)


def test_clear_expired_sess(auth_client, job_dict, create_session, db_session, fast_session_expiry):
    session1, session2 = create_session(), create_session()
    auth_client.bulk_post("/jobs/", [job_dict() for _ in range(10)])
    auth_client.bulk_put("/jobs/", {"state": "PREPROCESSED"})

    # Session1 acquires 5 jobs
    auth_client.post(
        f"/sessions/{session1.id}",
        max_wall_time_min=40,
        filter_tags={},
        max_num_jobs=5,
        max_nodes_per_job=8,
        check=status.HTTP_200_OK,
    )

    # Session2 acquires the other 5
    auth_client.post(
        f"/sessions/{session2.id}",
        max_wall_time_min=40,
        filter_tags={},
        max_num_jobs=5,
        max_nodes_per_job=8,
        check=status.HTTP_200_OK,
    )

    # All are locked
    assert db_session.query(models.Job).filter_by(session_id=session1.id).count() == 5
    assert db_session.query(models.Job).filter_by(session_id=session2.id).count() == 5

    time.sleep(FAST_EXPIRATION_PERIOD / 2)

    # Session2 ticks
    auth_client.put(f"/sessions/{session2.id}")
    time.sleep(FAST_EXPIRATION_PERIOD / 2)

    # Session2 ticks again
    auth_client.put(f"/sessions/{session2.id}")
    time.sleep(FAST_EXPIRATION_PERIOD / 2)

    # By now, session1 is cleared because it expired
    sess1_id = session1.id
    db_session.expire_all()
    assert db_session.query(models.Job).filter_by(session_id=sess1_id).count() == 0
    assert db_session.query(models.Job).filter_by(session_id=session2.id).count() == 5
    assert db_session.query(models.Session).count() == 1

    # If session1 tries to tick at this point, it will get a 404: lock is gone
    auth_client.put(f"/sessions/{sess1_id}", check=status.HTTP_404_NOT_FOUND)


def test_view_session_list(auth_client, job_dict, create_session):
    session = create_session()
    auth_client.bulk_post("/jobs/", [job_dict() for _ in range(5)])
    auth_client.bulk_put("/jobs/", {"state": "PREPROCESSED"})

    before_acquire = datetime.utcnow()

    # Session acquires 5 jobs
    auth_client.post(
        f"/sessions/{session.id}",
        max_wall_time_min=40,
        filter_tags={},
        max_num_jobs=5,
        max_nodes_per_job=8,
        check=status.HTTP_200_OK,
    )
    after_acquire = datetime.utcnow()
    sessions = auth_client.get("/sessions/")["results"]
    assert len(sessions) == 1
    sess = sessions[0]
    assert sess["batch_job_id"] == session.batch_job_id
    assert isoparse(sess["heartbeat"]) > before_acquire
    assert isoparse(sess["heartbeat"]) < after_acquire


def test_delete_session_frees_lock_on_all_jobs(auth_client, job_dict, create_session, db_session):
    session1, session2 = create_session(), create_session()
    auth_client.bulk_post("/jobs/", [job_dict() for _ in range(10)])
    auth_client.bulk_put("/jobs/", {"state": "PREPROCESSED"})

    # Session1 acquires 5 jobs
    auth_client.post(
        f"/sessions/{session1.id}",
        max_wall_time_min=40,
        filter_tags={},
        max_num_jobs=5,
        max_nodes_per_job=8,
        check=status.HTTP_200_OK,
    )
    # Session2 acquires 5 jobs
    auth_client.post(
        f"/sessions/{session2.id}",
        max_wall_time_min=40,
        filter_tags={},
        max_num_jobs=5,
        max_nodes_per_job=8,
        check=status.HTTP_200_OK,
    )
    assert db_session.query(models.Job).filter_by(session_id=session1.id).count() == 5
    assert db_session.query(models.Job).filter_by(session_id=session2.id).count() == 5

    # Session1 is deleted, leaving its jobs unlocked
    auth_client.delete(f"/sessions/{session1.id}")
    db_session.expire_all()
    unlocked = db_session.query(models.Job).filter(models.Job.session_id.is_(None))
    assert unlocked.count() == 5
    assert db_session.query(models.Job).filter_by(session_id=session2.id).count() == 5


def test_update_transfer_item(auth_client, job_dict, db_session):
    """Can update state, status_message, task_id"""
    job = auth_client.bulk_post(
        "/jobs/",
        [
            job_dict(
                transfers={
                    "hello-input": {
                        "location_alias": "MyCluster",
                        "path": "/path/to/input.dat",
                    }
                },
            )
        ],
    )[0]

    resp = auth_client.get("/transfers")
    assert resp["count"] == 1

    transfer_item = resp["results"][0]
    assert transfer_item["state"] == "pending"
    assert transfer_item["job_id"] == job["id"]
    assert transfer_item["direction"] == "in"
    assert transfer_item["local_path"] == "hello.yml"
    assert str(transfer_item["remote_path"]) == "/path/to/input.dat"
    assert transfer_item["task_id"] == ""
    assert transfer_item["transfer_info"] == {}

    # Update state and task id
    tid = transfer_item["id"]
    new_globus_task_id = uuid4().hex
    auth_client.put(f"/transfers/{tid}", state="active", task_id=new_globus_task_id)

    # TransferItem is indeed updated
    transfer = db_session.query(models.TransferItem).one()
    assert transfer.state == "active"
    assert transfer.task_id == new_globus_task_id


def test_finished_transfer_updates_job_state(auth_client, job_dict):
    """Can update state, status_message, task_id"""
    job = auth_client.bulk_post(
        "/jobs/",
        [
            job_dict(
                transfers={
                    "hello-input": {
                        "location_alias": "MyCluster",
                        "path": "/path/to/input.dat",
                    }
                },
            )
        ],
    )[0]

    assert job["state"] == "READY"
    transfer_item = auth_client.get("/transfers")["results"][0]
    tid = transfer_item["id"]
    auth_client.put(f"/transfers/{tid}", state="done", task_id=uuid4().hex)

    # After finishing stage-in, job was auto-marked STAGED_IN:
    job = auth_client.get(f"/jobs/{job['id']}")
    assert job["state"] == "STAGED_IN"


# Viewing State History
def test_aggregated_state_history(auth_client, job_dict):
    auth_client.bulk_post("/jobs/", [job_dict() for _ in range(10)])
    auth_client.bulk_put("/jobs/", {"state": "STAGED_IN"})
    auth_client.bulk_put("/jobs/", {"state": "PREPROCESSED"})
    events = auth_client.get("/events/")
    assert events["count"] == 10 * 3


def test_aggregated_state_history_by_batch_job(auth_client, create_session, job_dict):
    auth_client.bulk_post("/jobs/", [job_dict() for _ in range(2)])
    auth_client.bulk_put("/jobs/", {"state": "STAGED_IN"})
    auth_client.bulk_put("/jobs/", {"state": "PREPROCESSED"})

    session = create_session()
    session2 = create_session()

    # Batch Job 1 started and acquired all jobs:
    acquired = auth_client.post(
        f"/sessions/{session.id}",
        max_wall_time_min=40,
        filter_tags={},
        max_num_jobs=2,
        max_nodes_per_job=8,
        check=status.HTTP_200_OK,
    )
    assert len(acquired) == 2
    for job in acquired:
        assert job["batch_job_id"] == session.batch_job_id

    # Status updates:
    auth_client.put(f"/batch-jobs/{session.batch_job_id}", scheduler_id=31415, status="running")
    auth_client.bulk_put("/jobs/", {"state": "RUNNING"})

    # Can look up events by scheduler id
    events = auth_client.get("/events/", scheduler_id=31415)
    assert events["count"] == 2 * 4

    # Or by batch_job_id
    events = auth_client.get("/events/", batch_job_id=session.batch_job_id)
    print(*[(e["job_id"], e["to_state"]) for e in events["results"]], sep="\n")
    print(len(events["results"]), "events received")
    assert events["count"] == 2 * 4

    # Batch Job 2 has no associated events
    events2 = auth_client.get("/events/", batch_job_id=session2.id)
    assert events2["count"] == 0


def test_aggregated_state_history_by_tags(auth_client, job_dict):
    specs = [job_dict(tags={"foo": f"x{i}"}) for i in range(3)]
    jobs = auth_client.bulk_post("/jobs/", specs)

    for i in range(3):
        events = auth_client.get("/events/", tags=f"foo:x{i}")
        assert events["count"] == 1
        for event in events["results"]:
            assert event["job_id"] == jobs[i]["id"]

    events = auth_client.get("/events/")
    assert events["count"] == len(events["results"]) == 3


def test_aggregated_state_history_by_date_range(auth_client, job_dict):
    before_create = datetime.utcnow()
    auth_client.bulk_post("/jobs/", [job_dict() for _ in range(3)])
    time.sleep(0.1)
    after_create = datetime.utcnow()

    events = auth_client.get("/events/", timestamp_before=before_create)
    assert events["count"] == 0

    events = auth_client.get("/events/", timestamp_after=after_create)
    assert events["count"] == 0

    events = auth_client.get("/events/", timestamp_after=before_create, timestamp_before=after_create)
    assert events["count"] == 3


def test_can_delete_job(auth_client, job_dict):
    # Create 4 jobs: 2 PREPROCESSED, 2 STAGED_IN
    jobs = auth_client.bulk_post("/jobs/", [job_dict() for _ in range(4)])
    auth_client.bulk_put("/jobs/", {"state": "STAGED_IN"}, id=[j["id"] for j in jobs[:2]])
    auth_client.bulk_put("/jobs/", {"state": "PREPROCESSED"}, id=[j["id"] for j in jobs[2:]])
    # Bulk-Delete the STAGED_IN
    auth_client.bulk_delete("/jobs/", state="STAGED_IN")
    jobs = auth_client.get("/jobs/")
    assert jobs["count"] == 2
    for job in jobs["results"]:
        assert job["state"] == "PREPROCESSED"


def test_can_do_multiple_lookup_by_pk(auth_client, linear_dag):
    A, B, C = linear_dag
    ids = [j["id"] for j in (A, C)]
    jobs = auth_client.get("/jobs", id=ids)
    assert jobs["count"] == len(jobs["results"]) == 2


def test_can_traverse_dag(auth_client, linear_dag):
    A, B, C = linear_dag

    child_of_A = auth_client.get("/jobs", parent_id=A["id"])
    assert child_of_A["results"][0]["id"] == B["id"]

    child_of_B = auth_client.get("/jobs", parent_id=B["id"])
    assert child_of_B["results"][0]["id"] == C["id"]


def test_delete_recursively_deletes_children(auth_client, linear_dag, db_session):
    A, B, C = linear_dag
    assert db_session.query(models.Job).count() == 3
    auth_client.delete(f"/jobs/{A['id']}")
    db_session.expire_all()
    assert db_session.query(models.Job).count() == 0


def test_delete_recursively_deletes_children2(auth_client, linear_dag, db_session):
    A, B, C = linear_dag
    assert db_session.query(models.Job).count() == 3
    auth_client.delete(f"/jobs/{B['id']}")
    db_session.expire_all()
    assert db_session.query(models.Job).count() == 1


def test_cannot_acquire_with_another_lock_id(auth_client, create_session, job_dict, fastapi_user_test_client):
    """Passing a lock id that belongs to another user results in acquire() error"""
    # self.user (via self.client) has 10 jobs
    session = create_session()
    auth_client.bulk_post("/jobs/", [job_dict() for _ in range(3)])
    assert auth_client.get("/jobs")["count"] == 3

    other_client = fastapi_user_test_client()
    assert other_client.get("/jobs")["count"] == 0
    other_client.post(
        f"/sessions/{session.id}",
        max_wall_time_min=120,
        filter_tags={},
        max_num_jobs=5,
        max_nodes_per_job=32,
        check=status.HTTP_404_NOT_FOUND,
    )


def test_filter_transfers_by_state(site, auth_client, job_dict):
    """Can filter TransferItems by state list"""
    app = create_app(
        auth_client,
        site["id"],
        transfers={
            "hello-input": {
                "required": False,
                "direction": "in",
                "local_path": "hello.yml",
                "description": "Input file for SayHello",
            },
            "hello-output": {
                "required": True,
                "direction": "out",
                "local_path": "hello.results.xml",
                "description": "Results of SayHello",
            },
        },
    )
    job1 = job_dict(
        app_id=app["id"],
        transfers={
            "hello-input": {
                "location_alias": "MyCluster",
                "path": "/path/to/input.dat",
            },
            "hello-output": {
                "location_alias": "MyCluster",
                "path": "/path/to/result.xml",
            },
        },
    )
    job2 = job_dict(
        app_id=app["id"],
        transfers={
            "hello-output": {
                "location_alias": "MyCluster",
                "path": "/path/to/result.xml",
            }
        },
    )
    job1, job2 = auth_client.bulk_post("/jobs/", [job1, job2])
    assert job1["state"] == "READY"
    assert job2["state"] == "STAGED_IN"

    transfers = auth_client.get("/transfers", state="pending")
    assert transfers["count"] == 1

    transfers = auth_client.get("/transfers", state=["pending"])
    assert transfers["count"] == 1

    transfers = auth_client.get("/transfers", state=["awaiting_job"])
    assert transfers["count"] == 2

    transfers = auth_client.get("/transfers", state=["done", "error"])
    assert transfers["count"] == 0

    transfers = auth_client.get("/transfers", state=["awaiting_job", "pending"])
    assert transfers["count"] == 3


def test_postprocessed_job_with_stage_outs(site, auth_client, job_dict):
    """POSTPROCESSED job is acquired for stage-out before marking FINISHED"""
    app = create_app(
        auth_client,
        site["id"],
        transfers={
            "hello-input": {
                "required": False,
                "direction": "in",
                "local_path": "hello.yml",
                "description": "Input file for SayHello",
            },
            "hello-output": {
                "required": True,
                "direction": "out",
                "local_path": "hello.results.xml",
                "description": "Results of SayHello",
            },
        },
    )
    job1 = job_dict(
        app_id=app["id"],
        transfers={
            "hello-input": {
                "location_alias": "MyCluster",
                "path": "/path/to/input.dat",
            },
            "hello-output": {
                "location_alias": "MyCluster",
                "path": "/path/to/result.xml",
            },
        },
    )
    job2 = job_dict(
        app_id=app["id"],
        transfers={
            "hello-output": {
                "location_alias": "MyCluster",
                "path": "/path/to/result.xml",
            }
        },
    )
    job1, job2 = auth_client.bulk_post("/jobs/", [job1, job2])
    assert job1["state"] == "READY"
    assert job2["state"] == "STAGED_IN"

    job1 = auth_client.put(f"/jobs/{job1['id']}", state="POSTPROCESSED")
    assert job1["state"] == "POSTPROCESSED"
    job1 = auth_client.put(f"/jobs/{job1['id']}", state="STAGED_OUT")
    assert job1["state"] == "JOB_FINISHED"

    job2 = auth_client.put(f"/jobs/{job2['id']}", state="POSTPROCESSED")
    assert job2["state"] == "POSTPROCESSED"
    job2 = auth_client.put(f"/jobs/{job2['id']}", state="STAGED_OUT")
    assert job2["state"] == "JOB_FINISHED"


def test_reset_job_with_finished_parents(auth_client, linear_dag):
    A, B, C = linear_dag
    assert B["state"] == "AWAITING_PARENTS"

    A = auth_client.put(f"jobs/{A['id']}", state="POSTPROCESSED")
    assert A["state"] == "JOB_FINISHED"

    B = auth_client.get(f"jobs/{B['id']}")
    assert B["state"] == "READY"
    B = auth_client.put(f"jobs/{B['id']}", state="FAILED")
    B = auth_client.put(f"jobs/{B['id']}", state="RESET")
    assert B["state"] == "READY"
