from pathlib import Path
from pydantic import BaseModel, Field
from typing import Dict, Any, List
from enum import Enum
import uuid


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
    id: int
    job_id: int
    direction: TransferDirection
    local_path: Path
    remote_path: Path
    location_alias: str

    class Config:
        orm_mode = True


class PaginatedTransferItemOut(BaseModel):
    count: int
    results: List[TransferItemOut]


class TransferItemUpdate(TransferItemBase):
    state: TransferItemState = Field(None, example="active")


class TransferItemBulkUpdate(TransferItemUpdate):
    id: int
