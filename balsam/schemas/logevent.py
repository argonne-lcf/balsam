from pydantic import BaseModel
from typing import Dict, Any, List
from datetime import datetime


class LogEventOut(BaseModel):
    id: int
    job_id: int
    timestamp: datetime
    # Do not validate on the way out:
    from_state: str
    to_state: str
    data: Dict[str, Any]

    class Config:
        orm_mode = True


class PaginatedLogEventOut(BaseModel):
    count: int
    results: List[LogEventOut]
