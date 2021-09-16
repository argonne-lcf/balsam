import copy
import logging
import os
import random
import time
from pathlib import Path
from typing import Any
from uuid import uuid4

from locust import User, task  # type: ignore
from locust.env import Environment  # type: ignore
from locust.event import EventHook  # type: ignore

from balsam._api import models
from balsam.client import OAuthRequestsClient

logger = logging.getLogger(__name__)

TEST_TOKENS_FILE = os.environ.get("BALSAM_TEST_TOKENS", "test-tokens.txt")
if not Path(TEST_TOKENS_FILE).is_file():
    raise RuntimeError(
        f"Cannot find token file {TEST_TOKENS_FILE}. Create this or set the path with BALSAM_TEST_TOKENS."
    )

with open(TEST_TOKENS_FILE) as fp:
    tokens = [line.strip() for line in fp.readlines()]


class LocustBalsamClient(OAuthRequestsClient):
    def __init__(self, api_root: str, request_event: EventHook) -> None:
        super().__init__(api_root, token=random.choice(tokens))
        self._request_event = request_event

    def request(self, url: str, http_method: str, **kwargs: Any) -> Any:  # type: ignore
        start_time = time.perf_counter()
        url_parts = [(part if not part.isdigit() else ":id") for part in url.split("/")]
        url_name = "/".join(url_parts)
        request_meta = {
            "request_type": "balsam-requests-client",
            "name": f"{http_method} {url_name}",
            "response_length": 0,
            "response": None,
            "context": {},  # see HttpUser if you actually want to implement contexts
            "exception": None,
        }
        try:
            request_meta["response"] = super().request(url, http_method, **kwargs)
        except Exception as e:
            request_meta["exception"] = e
            raise
        finally:
            request_meta["response_time"] = (time.perf_counter() - start_time) * 1000
            self._request_event.fire(**request_meta)  # This is what makes the request actually get logged in Locust
        return request_meta["response"]


class BalsamUser(User):  # type: ignore
    def __init__(self, environment: Environment) -> None:
        super().__init__(environment)
        self.client = LocustBalsamClient("https://balsam-dev.alcf.anl.gov/", environment.events.request)
        self.Site = copy.deepcopy(models.Site)
        self.Site.objects = models.SiteManager(self.client)
        self.App = copy.deepcopy(models.App)  # Deepcopy
        self.App.objects = models.AppManager(self.client)
        self.BatchJob = copy.deepcopy(models.BatchJob)  # Deepcopy
        self.BatchJob.objects = models.BatchJobManager(self.client)
        self.Job = copy.deepcopy(models.Job)  # Deepcopy
        self.Job.objects = models.JobManager(self.client)
        self.TransferItem = copy.deepcopy(models.TransferItem)  # Deepcopy
        self.TransferItem.objects = models.TransferItemManager(self.client)
        self.Session = copy.deepcopy(models.Session)  # Deepcopy
        self.Session.objects = models.SessionManager(self.client)
        self.EventLog = copy.deepcopy(models.EventLog)  # Deepcopy
        self.EventLog.objects = models.EventLogManager(self.client)

    def on_start(self) -> None:
        # Clean up old Sites prior to testing
        for site in self.Site.objects.all():
            logger.debug(f"Deleting old site: {site}")
            site.delete()
            assert site.id is None
        logger.debug("OK I should have deleted all my sites by now!")
        failed_to_delete = list(self.Site.objects.all())
        assert len(failed_to_delete) == 0
        name = f"test-site{uuid4()}"
        path = Path("/projects/premade_site")
        self.premade_site = self.Site.objects.create(hostname=name, path=path)
        assert self.premade_site.id is not None
        class_path = "premade_app"
        parameters = {"foo": {"required": False, "default": "foo"}, "bar": {"required": False, "default": "bar"}}
        self.premade_app = self.App.objects.create(
            site_id=self.premade_site.id, class_path=class_path, parameters=parameters  # type: ignore
        )

    def on_stop(self) -> None:
        # Clean up Sites after testing
        for site in self.Site.objects.all():
            site.delete()

    @task(1)
    def site_create(self) -> None:
        num_sites = len(self.Site.objects.all())
        name = f"site_{num_sites}"
        path = f"/projects/foo/{uuid4()}"
        self.Site.objects.create(hostname=name, path=path)

    @task(3)
    def site_list(self) -> None:
        list(self.Site.objects.all())

    @task(1)
    def app_create(self):
        site = self.premade_site
        num_apps = len(self.App.objects.filter(site_id=site.id))
        class_path = f"foo.bar_{num_apps:08d}"
        parameters = {"foo": {"required": False, "default": "foo"}, "bar": {"required": False, "default": "bar"}}
        self.App.objects.create(
            site_id=site.id,
            class_path=class_path,
            parameters=parameters,
        )

    @task(3)
    def app_list(self) -> None:
        list(self.App.objects.all())

    @task(10)
    def job_create(self) -> None:
        app = self.premade_app

        parameters = {"foo": "yes", "bar": "maybe"}
        job_num = len(self.Job.objects.all())
        self.Job.objects.create("test/run_%08d" % job_num, app_id=app.id, parameters=parameters)

    @task(10)
    def job_list(self) -> None:
        list(self.Job.objects.all())

    @task(10)
    def job_list_site(self) -> None:
        list(self.Job.objects.filter(site_id=self.premade_site.id))

    @task(10)
    def bulk_job_submission(self) -> None:
        site = self.premade_site
        app = self.premade_app
        # create a bunch of jobs:
        parameters = {"foo": "yes", "bar": "maybe"}
        batch_id = str(int(time.time()))
        # create between 1 and 100 jobs
        jobs = [
            self.Job(
                f"batchjob_{batch_id}/run_{i:08d}",
                app_id=app.id,
                parameters=parameters,
                tags={"batch_id": batch_id},
            )
            for i in range(random.choice(range(128, 512)))
        ]
        jobs = self.Job.objects.bulk_create(jobs)

        # move jobs to PREPROCESSED state
        for job in jobs:
            job.state = "PREPROCESSED"
        self.Job.objects.bulk_update(jobs)

        # create batch job
        simulated_nodes = 128
        batch_job = self.BatchJob.objects.create(
            site_id=site.id,
            project="datascience",
            queue="default",
            num_nodes=simulated_nodes,
            wall_time_min=30,
            job_mode="mpi",
        )
        # create session
        sess = self.Session.objects.create(batch_job_id=batch_job.id, site_id=site.id)

        # associate jobs with batchjob
        sess.acquire_jobs(
            max_num_jobs=1024,
            filter_tags={"batch_id": batch_id},
            states=["PREPROCESSED", "RESTART_READY"],
        )

        # session heartbeat
        sess.tick()

        steps = int(simulated_nodes / len(jobs)) + 1
        for step in range(steps):

            start = simulated_nodes * step
            end = min(simulated_nodes * (step + 1), len(jobs))

            # change jobs to running state
            for job in jobs[start:end]:
                job.state = "RUNNING"
            self.Job.objects.bulk_update(jobs[start:end])

            # session heartbeat
            sess.tick()

            # change jobs to done or error state
            for job in jobs[start:end]:
                job.state = "RUN_DONE" if random.random() < 0.95 else "RUN_ERROR"
            self.Job.objects.bulk_update(jobs[start:end])

            # session heartbeat
            sess.tick()

        # Delete session when exiting:
        sess.delete()
