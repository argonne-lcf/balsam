from datetime import datetime
from enum import Enum
from typing import Any, Dict, List

from pydantic import BaseModel, Field


class EventOrdering(str, Enum):
    timestamp = "timestamp"
    timestamp_desc = "-timestamp"


class LogEventOut(BaseModel):
    id: int = Field(...)
    job_id: int = Field(...)
    timestamp: datetime = Field(...)
    # Do not validate on the way out:
    from_state: str = Field(...)
    to_state: str = Field(...)
    data: Dict[str, Any] = Field(...)

    class Config:
        orm_mode = True


class PaginatedLogEventOut(BaseModel):
    count: int
    results: List[LogEventOut]
