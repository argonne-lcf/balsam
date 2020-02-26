from datetime import datetime
import pathlib
import pytz
from typing import Union, List, Tuple
from pydantic import validator, root_validator
from .base_model import BalsamModel
from .query import Manager


def utc_datetime():
    return datetime.utcnow().replace(tzinfo=pytz.UTC)


class Job(BalsamModel):
    name: str
    workflow: str
    num_nodes: int
    cpu_affinity = "depth"


class JobManager(Manager):
    model_class = Job


class SiteStatus(BalsamModel):
    num_nodes: int = 0
    num_idle_nodes: int = 0
    num_busy_nodes: int = 0
    num_down_nodes: int = 0
    backfill_windows: List[Tuple[int, int]] = [(0, 0)]
    queued_jobs: List[Tuple[int, int, str]] = [(0, 0, "")]


class Site(BalsamModel):
    pk: Union[int, None] = None
    hostname: str
    path: pathlib.Path
    last_refresh: datetime = None
    status: SiteStatus = SiteStatus
    apps: List[str] = [""]

    @validator("last_refresh", pre=True, always=True)
    def default_refresh(cls, v):
        return v or utc_datetime()


class SiteManager(Manager):
    model_class = Site
    bulk_create_enabled = False
    bulk_update_enabled = False
    bulk_delete_enabled = False


class AppBackend(BalsamModel):
    site_hostname: str
    site_path: pathlib.Path
    site: Union[Site, int]
    class_name: str

    @root_validator(pre=True)
    def validate_site(cls, values):
        site = values["site"]
        if isinstance(site, Site):
            values["site_hostname"] = site.hostname
            values["site_path"] = site.path
            values["site"] = site.pk
        return values


class App(BalsamModel):
    pk: Union[int, None] = None
    name: str
    description: str = ""
    parameters: List[str] = []
    owner: str = ""
    users: List[str] = []
    backends: List[AppBackend]

    @validator("backends")
    def validate_backends(cls, v):
        if not isinstance(v, list):
            v = [v]
        return v


class AppManager(Manager):
    model_class = App
    bulk_create_enabled = False
    bulk_update_enabled = False
    bulk_delete_enabled = False
