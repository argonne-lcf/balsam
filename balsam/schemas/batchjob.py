from datetime import datetime
from pydantic import BaseModel, Field
from typing import Dict, List, Optional
from enum import Enum


class JobMode(str, Enum):
    serial = "serial"
    mpi = "mpi"


class BatchJobState(str, Enum):
    pending_submission = "pending_submission"
    queued = "queued"
    running = "running"
    finished = "finished"
    submit_failed = "submit_failed"
    pending_deletion = "pending_deletion"


class BatchJobBase(BaseModel):
    num_nodes: int = Field(..., example=128)
    wall_time_min: int = Field(..., example=60)
    job_mode: JobMode = Field(..., example="mpi")
    optional_params: Dict[str, str] = Field({}, example={"require_ssds": "1"})
    filter_tags: Dict[str, str] = Field(
        {}, example={"system": "H2O", "calc_type": "energy"}
    )


class BatchJobCreate(BatchJobBase):
    site_id: int = Field(..., example=4)
    project: str = Field(..., example="datascience")
    queue: str = Field(..., example="default")


class BatchJobUpdate(BaseModel):
    scheduler_id: int = Field(None, example=14192)
    state: BatchJobState = Field(None, example="queued")
    status_info: Dict[str, str] = Field(
        None, example={"error": "User is not a member of project X"}
    )
    start_time: Optional[datetime] = Field(None)
    end_time: Optional[datetime] = Field(None)


class BatchJobBulkUpdate(BatchJobUpdate):
    id: int = Field(..., example=8)


class BatchJobOut(BatchJobBase):
    id: int = Field(...)
    site_id: int = Field(...)
    scheduler_id: Optional[int] = Field(...)
    project: str = Field(...)
    queue: str = Field(...)
    state: BatchJobState = Field(...)
    status_info: Dict[str, str] = Field(...)
    start_time: Optional[datetime] = Field(...)
    end_time: Optional[datetime] = Field(...)

    class Config:
        orm_mode = True


class PaginatedBatchJobOut(BaseModel):
    count: int
    results: List[BatchJobOut]
