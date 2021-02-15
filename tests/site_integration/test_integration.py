import time
from typing import List

import pytest

from balsam._api.models import BatchJob, Job
from balsam.client import RESTClient
from balsam.config import SiteConfig
from balsam.schemas import JobMode


def _liveness_check(batch_job: BatchJob, timeout: float = 10.0, check_period: float = 1.0) -> bool:
    """
    Poll API until Launcher is running
    """
    num_checks = int(timeout / check_period)
    for _ in range(num_checks):
        batch_job.refresh_from_db()
        if batch_job.state == "running":
            return True
        elif batch_job.state in ["pending_submission", "queued"]:
            time.sleep(check_period)
        else:
            raise RuntimeError(f"Launcher job is over: state {batch_job.state}")
    raise RuntimeError(f"Launcher did not start in {timeout} seconds.")


@pytest.fixture(scope="class")
def single_node_mpi_batch_job(run_service: SiteConfig) -> BatchJob:
    """
    Submit 1 node MPI-mode launcher and block until live
    Launcher can be reused within a class.
    """
    BatchJob = run_service.client.BatchJob
    batch_job = BatchJob.objects.create(
        num_nodes=1,
        wall_time_min=5,
        job_mode=JobMode.mpi,
        site_id=run_service.settings.site_id,
        project="local",
        queue="local",
    )
    _liveness_check(batch_job, timeout=10.0, check_period=1.0)
    return batch_job


@pytest.fixture(scope="function")
def single_node_mpi_alive(single_node_mpi_batch_job: BatchJob) -> bool:
    return _liveness_check(single_node_mpi_batch_job, timeout=1.0, check_period=1.0)


@pytest.fixture(scope="class")
def client(run_service: SiteConfig) -> RESTClient:
    return run_service.client


def poll_until_state(jobs: List[Job], state: str, timeout=10.0, check_period=1.0) -> bool:
    """Refresh a list of jobs until they reach the specified state"""
    num_checks = int(timeout / check_period)
    for _ in range(num_checks):
        for job in jobs:
            job.refresh_from_db()
        if all(job.state == state for job in jobs):
            return True
        else:
            time.sleep(check_period)
    return False


def test_blank(run_service: SiteConfig) -> None:
    cf = run_service
    print("RUNNING A BALSAM TEST SITE\n", cf.settings)
    print("site path:", cf.site_path)
    assert 1


@pytest.mark.usefixtures("single_node_mpi_alive")
class TestSingleNodeMPIMode:
    def test_one_job(self, client: RESTClient) -> None:
        app = client.App.objects.first()
        job = client.Job.objects.create(
            "foo/1",
            app.id,
            parameters={"name": "world!"},
        )
        assert job.state == "STAGED_IN"
        assert job.id is not None
        poll_until_state([job], "JOB_FINISHED")
        assert job.state == "JOB_FINISHED"

    @pytest.mark.parametrize("num_jobs", [3])
    def test_multi_job(self, balsam_site_config: SiteConfig, num_jobs: int, client: RESTClient) -> None:
        app = client.App.objects.first()
        jobs: List[Job] = []
        for i in range(num_jobs):
            job = client.Job.objects.create(
                f"bar/{i}",
                app.id,
                parameters={"name": f"world{i}!"},
                node_packing_count=16,
            )
            jobs.append(job)
        assert all(job.state == "STAGED_IN" for job in jobs)
        poll_until_state(jobs, "JOB_FINISHED", timeout=15)
        for i, job in enumerate(jobs):
            assert job.state in ["JOB_FINISHED", "RUN_DONE"]
            stdout = job.resolve_workdir(balsam_site_config.data_path).joinpath("job.out").read_text()
            job.state
            assert f"world{i}" in stdout


@pytest.mark.alcf_theta
def test_cc_depth() -> None:
    assert 1
