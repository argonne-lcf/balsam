from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, Field


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


class SchedulerJobStatus(BaseModel):
    scheduler_id: int
    state: BatchJobState
    queue: str
    num_nodes: int
    wall_time_min: int
    project: str
    time_remaining_min: int


class SchedulerBackfillWindow(BaseModel):
    num_nodes: int
    wall_time_min: int


class SchedulerJobLog(BaseModel):
    start_time: Optional[datetime]
    end_time: Optional[datetime]


class BatchJobPartition(BaseModel):
    job_mode: JobMode = Field(..., example="mpi")
    num_nodes: int = Field(..., example=128)
    filter_tags: Dict[str, str] = Field({}, example={"system": "H2O", "calc_type": "energy"})


class BatchJobBase(BaseModel):
    num_nodes: int = Field(..., example=128)
    wall_time_min: int = Field(..., example=60)
    job_mode: JobMode = Field(..., example="mpi")
    optional_params: Dict[str, str] = Field({}, example={"require_ssds": "1"})
    filter_tags: Dict[str, str] = Field({}, example={"system": "H2O", "calc_type": "energy"})
    partitions: Optional[List[BatchJobPartition]] = Field(
        None,
        example=[
            {"job_mode": "mpi", "num_nodes": 1, "filter_tags": {"sim_type": "driver"}},
            {
                "job_mode": "serial",
                "num_nodes": 127,
                "filter_tags": {"sim_type": "worker"},
            },
        ],
    )

    class Config:
        use_enum_values = True


class BatchJobCreate(BatchJobBase):
    site_id: int = Field(..., example=4)
    project: str = Field(..., example="datascience")
    queue: str = Field(..., example="default")


class BatchJobUpdate(BaseModel):
    scheduler_id: int = Field(None, example=14192)
    state: BatchJobState = Field(None, example="queued")
    status_info: Dict[str, str] = Field(None, example={"error": "User is not a member of project X"})
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
