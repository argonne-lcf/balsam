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
    state: TransferItemState = Field(..., description="Status of this transfer item", example="active")
    task_id: str = Field("", example=uuid.uuid4(), description="Transfer Task ID used to lookup transfer item status")
    transfer_info: Dict[str, Any] = Field(
        {}, example={"bandwidth": "4028"}, description="Arbitrary transfer state info"
    )


class TransferItemOut(TransferItemBase):
    id: int = Field(..., description="Transfer item id", example=2)
    job_id: int = Field(..., description="Associated Job id", example=123)
    direction: TransferDirection = Field(..., description="Transfer direction (in/out)", example="in")
    local_path: Path = Field(..., description="Path relative to the Job workdir", example="input.json")
    remote_path: Path = Field(
        ..., description="Absolute path on the remote filesystem", example="/path/to/input.json"
    )
    location_alias: str = Field(..., description="Site-defined alias of the remote location", example="APS-DTN")
    recursive: bool = Field(..., description="Whether or not this item is a directory", example=False)

    class Config:
        orm_mode = True


class PaginatedTransferItemOut(BaseModel):
    count: int
    results: List[TransferItemOut]


class TransferItemUpdate(TransferItemBase):
    state: TransferItemState = Field(None, example="active", description="Status of this transfer item")


class TransferItemBulkUpdate(TransferItemUpdate):
    id: int
