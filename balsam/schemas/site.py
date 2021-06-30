from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional
from uuid import UUID

from pydantic import AnyUrl, BaseModel, Field, validator

from .batchjob import SchedulerBackfillWindow, SchedulerJobStatus


class QueuedJobState(str, Enum):
    queued = "queued"
    running = "running"


class TransferProtocol(str, Enum):
    globus = "globus"
    rsync = "rsync"


class BackfillWindow(BaseModel):
    queue: str = Field(..., example="default", description="Queue name to which Jobs may be submitted")
    num_nodes: int = Field(..., example=130, description="Number of idle nodes")
    wall_time_min: int = Field(..., example=40, description="Duration in minutes that nodes will remain idle")


class QueuedJob(BaseModel):
    queue: str = Field(..., example="default", description="Name of queue")
    num_nodes: int = Field(..., example=128, description="Number of nodes allocated")
    wall_time_min: int = Field(..., example=60, description="Length of node allocation")
    state: QueuedJobState = Field(..., example=QueuedJobState.queued, description="Status of the allocation")


class AllowedQueue(BaseModel):
    max_nodes: int = Field(..., example=8)
    max_walltime: int = Field(..., example=60)
    max_queued_jobs: int = Field(..., example=1)


class SiteBase(BaseModel):
    hostname: str = Field(
        ..., example="thetalogin3.alcf.anl.gov", description="The Site network location, for human reference only"
    )
    path: Path = Field(
        ..., example="/projects/datascience/user/mySite", description="Absolute filesystem path of the Site"
    )
    globus_endpoint_id: Optional[UUID] = Field(None, description="Associated Globus endpoint ID")
    backfill_windows: Dict[str, List[SchedulerBackfillWindow]] = Field(
        {}, description="Idle backfill currently available at the Site, keyed by queue name"
    )
    queued_jobs: Dict[int, SchedulerJobStatus] = Field(
        {}, description="Queued scheduler jobs at the Site, keyed by scheduler ID"
    )
    optional_batch_job_params: Dict[str, str] = Field(
        {},
        example={"enable_ssh": 1},
        description="Optional pass-through parameters accepted by the Site batchjob template",
    )
    allowed_projects: List[str] = Field(
        [],
        example=["datascience", "materials-adsp"],
        description="Allowed projects/allocations for batchjob submission",
    )
    allowed_queues: Dict[str, AllowedQueue] = Field(
        {},
        example={
            "debug-cache-quad": {
                "max_nodes": 8,
                "max_walltime": 60,
                "max_queued_jobs": 1,
            }
        },
        description="Allowed queues and associated queueing policies",
    )
    transfer_locations: Dict[str, AnyUrl] = Field(
        {},
        example={
            "APS-DTN": "globus://ddb59aef-6d04-11e5-ba46-22000b92c6ec",
            "MyCluster": "rsync://user@hostname.mycluster",
        },
        description="Trusted transfer location aliases and associated protocol/URLs",
    )

    @validator("path")
    def path_is_absolute(cls, v: Path) -> Path:
        if not v.is_absolute():
            raise ValueError("path must be absolute")
        return v


class SiteCreate(SiteBase):
    pass


class SiteUpdate(SiteBase):
    hostname: Optional[str] = Field(None, example="thetalogin3.alcf.anl.gov", description="The Site network location, for human reference only")  # type: ignore
    path: Optional[Path] = Field(None, example="/projects/datascience/user/mySite", description="Absolute filesystem path of the Site")  # type: ignore


class SiteOut(SiteBase):
    class Config:
        orm_mode = True

    id: int = Field(..., example=123)
    last_refresh: datetime = Field(..., example=datetime.utcnow())
    creation_date: datetime = Field(..., example=datetime.utcnow())


class PaginatedSitesOut(BaseModel):
    count: int
    results: List[SiteOut]
