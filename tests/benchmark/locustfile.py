import copy
import random
import time

from locust import User, task

from balsam._api import models
from balsam.client import OAuthRequestsClient

tokens = [
    line.strip()
    for line in open("/lus/theta-fs0/projects/datascience/parton/balsam2-dev/test-tokens.txt").readlines()
]


class LocustBalsamClient(OAuthRequestsClient):
    def __init__(self, api_root, token, request_event):
        super().__init__(api_root, token=random.choice(tokens))
        self._request_event = request_event

    def request(self, url, http_method, **kwargs):
        start_time = time.perf_counter()
        request_meta = {
            "request_type": "balsam-requests-client",
            "name": f"{http_method} {url}",
            "response_length": 0,
            "response": None,
            "context": {},  # see HttpUser if you actually want to implement contexts
            "exception": None,
        }
        try:
            request_meta["response"] = super().request(url, http_method, **kwargs)
        except Exception as e:
            request_meta["exception"] = e
        request_meta["response_time"] = (time.perf_counter() - start_time) * 1000
        self._request_event.fire(**request_meta)  # This is what makes the request actually get logged in Locust
        return request_meta["response"]


class BalsamUser(User):
    def __init__(self, environment):
        super().__init__(environment)
        self.client = LocustBalsamClient(
            "https://balsam-dev.alcf.anl.gov/", "DUMMY_TOKEN", environment.events.request
        )
        self.Site = copy.deepcopy(
            models.Site
        )  # Use DeepCopy; otherwise multiple threads will use the same "Site" client!
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
        name = "premade_site"
        path = "/projects/premade_site"
        self.premade_site = self.Site.objects.create(hostname=name, path=path)
        class_path = "premade_app"
        parameters = {"foo": {"required": False}, "bar": {"required": False}}
        self.premade_app = self.App.objects.create(
            site_id=self.premade_site.id, class_path=class_path, parameters=parameters
        )

    def on_stop(self) -> None:
        self.Site.objects.all().delete()

    @task(1)
    def site_create(self) -> None:
        num_sites = len(self.Site.objects.all())
        name = "site_%08d" % num_sites
        path = "/projects/foo/%08d" % num_sites
        self.Site.objects.create(hostname=name, path=path)

    @task(5)
    def site_list(self) -> None:
        list(self.Site.objects.all())

    @task(3)
    def app_create(self):
        site = self.premade_site
        num_apps = len(self.Site.objects.filter(id == site.id))
        class_path = "foo.bar_%08d" % num_apps
        parameters = {"foo": {"required": False}, "bar": {"required": False}}
        self.App.objects.create(
            site_id=site.id,
            class_path=class_path,
            parameters=parameters,
        )

    @task(5)
    def app_list(self) -> None:
        list(self.App.objects.all())

    @task(7)
    def job_create(self) -> None:
        app = self.premade_app

        parameters = {"foo": {"required": False}, "bar": {"required": False}}
        job_num = len(self.Job.objects.all())
        self.Job.objects.create("test/run_%08d" % job_num, app_id=app.id, parameters=parameters)

    @task(10)
    def job_list(self) -> None:
        list(self.Job.objects.all())

    @task(10)
    def job_list_site(self) -> None:
        list(self.Job.objects.filter(site_id=self.premade_site.id))

    @task(5)
    def bulk_job_submission(self) -> None:
        site = self.premade_site
        app = self.premade_app
        # create a bunch of jobs:
        jobs = []
        parameters = {"foo": {"required": False}, "bar": {"required": False}}
        batch_id = str(int(time.time()))
        # create between 1 and 100 jobs
        for i in range(random.choice(range(128, 512))):
            jobs.append(
                self.Job.objects.create(
                    f"batchjob_{batch_id}/run_{i:08d}",
                    app_id=app.id,
                    parameters=parameters,
                    tags={"batch_id": batch_id},
                )
            )

        # move jobs to PREPROCESSED state
        for job in jobs:
            job.state = "PREPROCESSED"
            job.save()

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
                job.save()

            # session heartbeat
            sess.tick()

            time.sleep(random.uniform(0.5, 5))

            # change jobs to done or error state
            for job in jobs[start:end]:
                job.state = "RUN_DONE" if random.random() < 0.95 else "RUN_ERROR"
                job.save()

            # session heartbeat
            sess.tick()
