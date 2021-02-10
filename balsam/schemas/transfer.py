import uuid
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List

from pydantic import BaseModel, Field


class TransferItemState(str, Enum):
    awaiting_job = "awaiting_job"
    pending = "pending"
    active = "active"
    inactive = "inactive"
    done = "done"
    error = "error"


class TransferProtocol(str, Enum):
    globus = "globus"
    rsync = "rsync"


class TransferDirection(str, Enum):
    stage_in = "in"
    stage_out = "out"


class TransferItemBase(BaseModel):
    state: TransferItemState = Field(..., example="active")
    task_id: str = Field("", example=uuid.uuid4())
    transfer_info: Dict[str, Any] = Field({}, example={"bandwidth": "4028"})


class TransferItemOut(TransferItemBase):
    id: int = Field(...)
    job_id: int = Field(...)
    direction: TransferDirection = Field(...)
    local_path: Path = Field(...)
    remote_path: Path = Field(...)
    location_alias: str = Field(...)
    recursive: bool = Field(...)

    class Config:
        orm_mode = True


class PaginatedTransferItemOut(BaseModel):
    count: int
    results: List[TransferItemOut]


class TransferItemUpdate(TransferItemBase):
    state: TransferItemState = Field(None, example="active")


class TransferItemBulkUpdate(TransferItemUpdate):
    id: int
