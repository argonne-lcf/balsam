# This file was auto-generated via /Users/misha/workflow/balsam/env/bin/python balsam/schemas/api_generator.py balsam.api.bases.SiteBase balsam.api.bases.SiteManagerMixin balsam.server.routers.filters.SiteQuery
# [git rev 354dae8]
# Do *not* make changes to the API by changing this file!

import typing
from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional
from uuid import UUID

import pydantic

import balsam.api.bases
import balsam.api.manager
import balsam.api.model
import balsam.server.routers.filters
from balsam.api.manager import Manager
from balsam.api.model import Field
from balsam.api.query import Query


class Site(balsam.api.bases.SiteBase):
    _create_model_cls = balsam.schemas.site.SiteCreate
    _update_model_cls = balsam.schemas.site.SiteUpdate
    _read_model_cls = balsam.schemas.site.SiteOut
    objects: "SiteManager"

    hostname = Field[str]()
    path = Field[Path]()
    globus_endpoint_id = Field[Optional[UUID]]()
    num_nodes = Field[int]()
    backfill_windows = Field[typing.List[balsam.schemas.site.BackfillWindow]]()
    queued_jobs = Field[typing.List[balsam.schemas.site.QueuedJob]]()
    optional_batch_job_params = Field[typing.Dict[str, str]]()
    allowed_projects = Field[typing.List[str]]()
    allowed_queues = Field[typing.Dict[str, balsam.schemas.site.AllowedQueue]]()
    transfer_locations = Field[typing.Dict[str, pydantic.networks.AnyUrl]]()
    id = Field[Optional[int]]()
    last_refresh = Field[Optional[datetime]]()
    creation_date = Field[Optional[datetime]]()

    def __init__(
        self,
        hostname: str,
        path: Path,
        globus_endpoint_id: Optional[UUID] = None,
        num_nodes: int = 0,
        backfill_windows: Optional[typing.List[balsam.schemas.site.BackfillWindow]] = None,
        queued_jobs: Optional[typing.List[balsam.schemas.site.QueuedJob]] = None,
        optional_batch_job_params: Optional[typing.Dict[str, str]] = None,
        allowed_projects: Optional[typing.List[str]] = None,
        allowed_queues: Optional[typing.Dict[str, balsam.schemas.site.AllowedQueue]] = None,
        transfer_locations: Optional[typing.Dict[str, pydantic.networks.AnyUrl]] = None,
        **kwargs: Any,
    ) -> None:
        _kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        _kwargs.update(kwargs)
        return super().__init__(**_kwargs)


class SiteQuery(Query[Site]):
    def get(
        self,
        hostname: Optional[str] = None,
        path: Optional[str] = None,
        id: Optional[typing.List[int]] = None,
    ) -> Site:
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._get(**kwargs)

    def filter(
        self,
        hostname: Optional[str] = None,
        path: Optional[str] = None,
        id: Optional[typing.List[int]] = None,
    ) -> "SiteQuery":
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._filter(**kwargs)

    def update(
        self,
        hostname: Optional[str] = None,
        path: Optional[Path] = None,
        globus_endpoint_id: Optional[UUID] = None,
        num_nodes: Optional[int] = None,
        backfill_windows: Optional[typing.List[balsam.schemas.site.BackfillWindow]] = None,
        queued_jobs: Optional[typing.List[balsam.schemas.site.QueuedJob]] = None,
        optional_batch_job_params: Optional[typing.Dict[str, str]] = None,
        allowed_projects: Optional[typing.List[str]] = None,
        allowed_queues: Optional[typing.Dict[str, balsam.schemas.site.AllowedQueue]] = None,
        transfer_locations: Optional[typing.Dict[str, pydantic.networks.AnyUrl]] = None,
    ) -> List[Site]:
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return self._update(**kwargs)


class SiteManager(Manager[Site], balsam.api.bases.SiteManagerMixin):
    _api_path = "sites/"
    _model_class = Site
    _query_class = SiteQuery

    def create(
        self,
        hostname: str,
        path: Path,
        globus_endpoint_id: Optional[UUID] = None,
        num_nodes: int = 0,
        backfill_windows: Optional[typing.List[balsam.schemas.site.BackfillWindow]] = None,
        queued_jobs: Optional[typing.List[balsam.schemas.site.QueuedJob]] = None,
        optional_batch_job_params: Optional[typing.Dict[str, str]] = None,
        allowed_projects: Optional[typing.List[str]] = None,
        allowed_queues: Optional[typing.Dict[str, balsam.schemas.site.AllowedQueue]] = None,
        transfer_locations: Optional[typing.Dict[str, pydantic.networks.AnyUrl]] = None,
    ) -> Site:
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return super()._create(**kwargs)

    def all(self) -> "SiteQuery":
        return self._query_class(manager=self)

    def get(
        self,
        hostname: Optional[str] = None,
        path: Optional[str] = None,
        id: Optional[typing.List[int]] = None,
    ) -> Site:
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return SiteQuery(manager=self).get(**kwargs)

    def filter(
        self,
        hostname: Optional[str] = None,
        path: Optional[str] = None,
        id: Optional[typing.List[int]] = None,
    ) -> "SiteQuery":
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"] and v is not None}
        return SiteQuery(manager=self).filter(**kwargs)
