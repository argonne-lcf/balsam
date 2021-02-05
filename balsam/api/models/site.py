from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union
from uuid import UUID

from pydantic import AnyUrl

from balsam import schemas
from balsam.api.manager_base import Manager
from balsam.api.model_base import BalsamModel, Field
from balsam.api.query import Query
from balsam.schemas.site import AllowedQueue, BackfillWindow, QueuedJob


class Site(BalsamModel):
    create_model_cls = schemas.SiteCreate
    update_model_cls = schemas.SiteUpdate
    read_model_cls = schemas.SiteOut
    objects: "SiteManager"

    id = Field[Optional[int]]()
    hostname = Field[str]()
    path = Field[Path]()
    globus_endpoint_id = Field[Optional[UUID]]()
    num_nodes = Field[int]()
    backfill_windows = Field[List[BackfillWindow]]()
    queued_jobs = Field[List[QueuedJob]]()
    optional_batch_job_params = Field[Dict[str, str]]()
    allowed_projects = Field[List[str]]()
    allowed_queues = Field[Dict[str, AllowedQueue]]()
    transfer_locations = Field[Dict[str, AnyUrl]]()
    last_refresh = Field[datetime]()
    creation_date = Field[datetime]()

    def __init__(
        self,
        hostname: str,
        path: Union[Path, str],
        globus_endpoint_id: Optional[UUID] = None,
        num_nodes: int = 0,
        backfill_windows: Optional[List[BackfillWindow]] = None,
        queued_jobs: Optional[List[QueuedJob]] = None,
        optional_batch_job_params: Optional[Dict[str, str]] = None,
        allowed_projects: Optional[List[str]] = None,
        allowed_queues: Optional[Dict[str, AllowedQueue]] = None,
        transfer_locations: Optional[Dict[str, str]] = None,
    ) -> None:
        kwargs = {k: v for k, v in locals().items() if k != "self" and v is not None}
        return super().__init__(**kwargs)


class SiteManager(Manager[Site]):
    path = "sites/"
    model_class = Site
    query_class = Query[Site]

    def create(
        self,
        hostname: str,
        path: Union[Path, str],
        globus_endpoint_id: Optional[UUID] = None,
        num_nodes: int = 0,
        backfill_windows: Optional[List[BackfillWindow]] = None,
        queued_jobs: Optional[List[QueuedJob]] = None,
        optional_batch_job_params: Optional[Dict[str, str]] = None,
        allowed_projects: Optional[List[str]] = None,
        allowed_queues: Optional[Dict[str, AllowedQueue]] = None,
        transfer_locations: Optional[Dict[str, str]] = None,
    ) -> Site:
        kwargs = {k: v for k, v in locals().items() if k != "self" and v is not None}
        return super()._create(**kwargs)

    def filter(
        self,
        hostname: Optional[str] = None,
        path: Union[str, Path, None] = None,
        id: Union[int, List[int], None] = None,
    ) -> "SiteQuery":
        kwargs = {k: v for k, v in locals().items() if k != "self" and v is not None}
        return SiteQuery(manager=self).filter(**kwargs)

    def get(
        self,
        hostname: Optional[str] = None,
        path: Union[str, Path, None] = None,
        id: Union[int, List[int], None] = None,
    ) -> Site:
        kwargs = {k: v for k, v in locals().items() if k != "self" and v is not None}
        return SiteQuery(manager=self).get(**kwargs)


class SiteQuery(Query[Site]):
    def get(
        self,
        hostname: Optional[str] = None,
        path: Union[str, Path, None] = None,
        id: Union[int, List[int], None] = None,
    ) -> Site:
        kwargs = {k: v for k, v in locals().items() if k != "self" and v is not None}
        return self._get(**kwargs)

    def filter(
        self,
        hostname: Optional[str] = None,
        path: Union[str, Path, None] = None,
        id: Union[int, List[int], None] = None,
    ) -> "SiteQuery":
        kwargs = {k: v for k, v in locals().items() if k != "self" and v is not None}
        return self._filter(**kwargs)

    def update(
        self,
        hostname: Optional[str] = None,
        path: Union[Path, str, None] = None,
        globus_endpoint_id: Optional[UUID] = None,
        num_nodes: int = 0,
        backfill_windows: Optional[List[BackfillWindow]] = None,
        queued_jobs: Optional[List[QueuedJob]] = None,
        optional_batch_job_params: Optional[Dict[str, str]] = None,
        allowed_projects: Optional[List[str]] = None,
        allowed_queues: Optional[Dict[str, AllowedQueue]] = None,
        transfer_locations: Optional[Dict[str, str]] = None,
    ) -> List[Site]:
        kwargs = {k: v for k, v in locals().items() if k != "self" and v is not None}
        return self._update(**kwargs)

    def order_by(self, *fields: str) -> "SiteQuery":
        return self._order_by(*fields)
