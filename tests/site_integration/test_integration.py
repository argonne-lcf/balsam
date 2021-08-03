import time
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, List

import pytest

from balsam._api.models import BatchJob, EventLog, Job
from balsam.client import RESTClient
from balsam.config import SiteConfig
from balsam.schemas import JobMode
from balsam.schemas.batchjob import BatchJobState

from ..test_platform import get_launcher_shutdown_timeout, get_launcher_startup_timeout


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


@pytest.fixture(scope="class", params=[JobMode.mpi, JobMode.serial])
def launcher_job(run_service: SiteConfig, request: Any) -> Iterable[BatchJob]:
    """
    Submit 1 node launcher and block until live.
    Launcher can be reused within a class.
    All dependent tests repeats for MPI mode and Serial mode.
    """
    BatchJob = run_service.client.BatchJob
    assert run_service.settings.scheduler is not None
    project = run_service.settings.scheduler.allowed_projects[0]
    queue = next(iter(run_service.settings.scheduler.allowed_queues))

    job_mode: JobMode = request.param
    batch_job = BatchJob.objects.create(
        num_nodes=1,
        wall_time_min=5,
        job_mode=job_mode,
        site_id=run_service.site_id,
        project=project,
        queue=queue,
    )
    print("Created BatchJob id:", batch_job.id, datetime.now())
    _liveness_check(batch_job, timeout=get_launcher_startup_timeout(), check_period=1.0)
    print("BatchJob started:", batch_job.id, datetime.now())
    yield batch_job
    batch_job.state = BatchJobState.pending_deletion
    batch_job.save()
    print("Killing BatchJob id:", batch_job.id, datetime.now())
    shutdown_timeout = int(get_launcher_shutdown_timeout())
    for _ in range(shutdown_timeout):
        time.sleep(1)
        batch_job.refresh_from_db()
        if batch_job.state == "finished":
            print(f"BatchJob {batch_job.id} id FINISHED at", datetime.now())
            return
    raise RuntimeError(f"Launcher did not end within {shutdown_timeout} seconds.")


@pytest.fixture(scope="function")
def live_launcher(launcher_job: BatchJob) -> bool:
    """
    Blocks until the launcher started.
    The launcher persists for the scope a class, but a quick liveness check is
    repeated before each test function.
    """
    return _liveness_check(launcher_job, timeout=1.0, check_period=1.0)


@pytest.fixture(scope="class")
def client(run_service: SiteConfig) -> RESTClient:
    return run_service.client


def poll_until_state(
    jobs: List[Job], state: str, fail_state: str = "FAILED", timeout: float = 10.0, check_period: float = 1.0
) -> bool:
    """Refresh a list of jobs until they reach the specified state"""
    num_checks = int(timeout / check_period)
    for _ in range(num_checks):
        for job in jobs:
            job.refresh_from_db()
        if all(job.state == state for job in jobs):
            return True
        if fail_state and any(job.state == fail_state for job in jobs):
            return False
        else:
            time.sleep(check_period)
    return False


@pytest.mark.usefixtures("live_launcher")
class TestSingleNodeMPIMode:
    @pytest.mark.parametrize("num_jobs", [3])
    def test_multi_job(self, balsam_site_config: SiteConfig, num_jobs: int, client: RESTClient) -> None:
        """
        3 hello world jobs run to completion
        """
        app = client.App.objects.get(class_path="hello.Hello")
        assert app.id is not None
        jobs: List[Job] = []
        for i in range(num_jobs):
            job = client.Job.objects.create(
                Path(f"bar/{i}"),
                app_id=app.id,
                parameters={"name": f"world{i}!"},
                node_packing_count=2,
            )
            jobs.append(job)
        print(f"Created {num_jobs} jobs at", datetime.now())
        assert all(job.state == "STAGED_IN" for job in jobs)
        print("Start polling for jobs to reach JOB_FINISHED at", datetime.now())
        poll_until_state(jobs, "JOB_FINISHED", timeout=300.0)
        print("Polling jobs DONE at", datetime.now())
        for i, job in enumerate(jobs):
            print(f"job {i}: {job.state}")
            assert job.state in [
                "JOB_FINISHED",
                "RUN_DONE",
            ], f"Job state: {job.state}\n{list(EventLog.objects.filter(job_id=job.id))}"
            stdout = job.resolve_workdir(balsam_site_config.data_path).joinpath("job.out").read_text()
            assert f"world{i}" in stdout


@pytest.mark.alcf_theta
def test_cc_depth() -> None:
    assert 1
