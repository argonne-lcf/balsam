from datetime import datetime
from pydantic import BaseModel
from typing import Dict, List


class SessionCreate(BaseModel):
    batch_job_id: int


class SessionOut(BaseModel):
    id: int
    batch_job_id: int
    heartbeat: datetime

    class Config:
        orm_mode = True


class JobAcquireSpec(BaseModel):
    min_nodes: int
    max_nodes: int
    serial_only: bool
    max_num_acquire: int


class SessionAcquire(BaseModel):
    max_wall_time_min: int
    acquire: List[JobAcquireSpec]
    filter_tags: Dict[str, str]
