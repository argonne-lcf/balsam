from datetime import datetime
from typing import Dict, List, Optional, Set

from pydantic import BaseModel, Field

from .job import RUNNABLE_STATES, JobState


class SessionCreate(BaseModel):
    site_id: int = Field(..., description="Site id of the running Session")
    batch_job_id: Optional[int] = Field(None, description="Associated batchjob id")


class SessionOut(BaseModel):
    id: int = Field(..., description="Session id")
    site_id: int = Field(..., description="Site id of the running Session")
    batch_job_id: Optional[int] = Field(None, description="Associated batchjob id")
    heartbeat: datetime = Field(..., description="Last heartbeat received from Session")

    class Config:
        orm_mode = True


class SessionAcquire(BaseModel):
    max_num_jobs: int
    max_wall_time_min: Optional[int]
    max_nodes_per_job: Optional[int]
    max_aggregate_nodes: Optional[float]
    serial_only: bool = False
    filter_tags: Dict[str, str]
    states: Set[JobState] = RUNNABLE_STATES
    app_ids: Set[int] = set()


class PaginatedSessionsOut(BaseModel):
    count: int
    results: List[SessionOut]
