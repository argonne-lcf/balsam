from datetime import datetime
from typing import Any, Dict, List

from pydantic import BaseModel


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
