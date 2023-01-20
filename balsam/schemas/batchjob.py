from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class JobMode(str, Enum):
    serial = "serial"
    mpi = "mpi"


class BatchJobOrdering(str, Enum):
    start_time = "start_time"
    start_time_desc = "-start_time"


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
    queued_time_min: int


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
    num_nodes: int = Field(..., example=128, description="Requested number of nodes for this allocation")
    wall_time_min: int = Field(..., example=60, description="Requested wall clock time for this allocation")
    job_mode: JobMode = Field(..., example="mpi", description="Balsam launcher execution mode (if single partition)")
    optional_params: Dict[str, str] = Field(
        {},
        example={"require_ssds": "1"},
        description="Optional pass-through parameters submitted with the batchjob script",
    )
    filter_tags: Dict[str, str] = Field(
        {}, example={"system": "H2O", "calc_type": "energy"}, description="Only run Jobs containing these tags"
    )
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
        description="Optionally, subdivide an allocation into multiple partitions.",
    )

    class Config:
        use_enum_values = True


class BatchJobCreate(BatchJobBase):
    site_id: int = Field(..., example=4, description="The Site id where this batchjob is submitted")
    project: str = Field(..., example="datascience", description="The project/allocation to charge for this batchjob")
    queue: str = Field(..., example="default", description="Which queue the batchjob is submitted on")


class BatchJobUpdate(BaseModel):
    scheduler_id: int = Field(None, example=14192, description="The local HPC scheduler's ID for this batchjob")
    state: BatchJobState = Field(
        None, example="queued", description="Status of this batchjob in the local HPC scheduler"
    )
    status_info: Dict[str, str] = Field(
        None, example={"error": "User is not a member of project X"}, description="Arbitrary status info"
    )
    start_time: Optional[datetime] = Field(None, description="BatchJob execution start time")
    end_time: Optional[datetime] = Field(None, description="BatchJob execution end time")


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
