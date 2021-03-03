import random
import uuid

from locust import HttpUser, between, task

from balsam.client import urls


def job_dict(workdir, app_id, tags={}, parameters={"name": "world"}):
    return dict(
        app_id=app_id,
        workdir=workdir,
        tags=tags,
        transfers={},
        parameters=parameters,
        num_nodes=1,
        ranks_per_node=1,
        threads_per_rank=1,
        threads_per_core=1,
        gpus_per_rank=0,
        node_packing_count=1,
        wall_time_min=0,
    )


class JobSubmitter(HttpUser):
    wait_time = between(0.5, 1.5)

    @task(1)
    def list_jobs(self) -> None:
        """
        Fetch up to 100 jobs
        """
        self.client.get("/jobs", params={"limit": 100, "state": "STAGED_IN"})

    @task(1)
    def get_me(self) -> None:
        self.client.get("/auth/me")

    @task(1)
    def submit_jobs(self) -> None:
        njobs = random.randrange(1, 10)
        job_dicts = [job_dict(f"test/{i}", app_id=self.app_id) for i in range(njobs)]
        self.client.post("/jobs", json=job_dicts)

    def on_start(self) -> None:
        username = f"user{uuid.uuid4()}"
        password = "foo"
        cred = {"username": username, "password": password}
        self.client.post("/" + urls.PASSWORD_REGISTER, json=cred)

        resp = self.client.post("/" + urls.PASSWORD_LOGIN, data=cred)
        access_token = resp.json()["access_token"]
        self.client.headers["Authorization"] = f"Bearer {access_token}"

        resp = self.client.post(
            "/sites",
            json={
                "hostname": "thetalogin4",
                "path": f"/path/to/site/{username}",
                "transfer_locations": {},
            },
        )
        self.site_id = resp.json()["id"]

        resp = self.client.post(
            "/apps",
            json={
                "site_id": self.site_id,
                "class_path": "demo.AppName",
                "parameters": {"name": {"required": True}},
                "transfers": {},
            },
        )
        self.app_id = resp.json()["id"]
