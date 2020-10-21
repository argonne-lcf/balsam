from datetime import datetime
from pydantic import BaseModel
from typing import Dict, List, Set, Optional
from .job import JobState, RUNNABLE_STATES


class SessionCreate(BaseModel):
    site_id: int
    batch_job_id: Optional[int]


class SessionOut(BaseModel):
    id: int
    site_id: int
    batch_job_id: Optional[int]
    heartbeat: datetime

    class Config:
        orm_mode = True


class SessionAcquire(BaseModel):
    max_num_jobs: int
    max_wall_time_min: int
    max_nodes_per_job: Optional[int]
    max_aggregate_nodes: Optional[float]
    serial_only: bool = False
    filter_tags: Dict[str, str]
    states: Set[JobState] = RUNNABLE_STATES


class PaginatedSessionsOut(BaseModel):
    count: int
    results: List[SessionOut]
