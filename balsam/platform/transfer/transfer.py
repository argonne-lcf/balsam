import ABC
from pathlib import Path
from enum import Enum
from typing import List, Tuple, Union, Dict, Any
from uuid import UUID
from pydantic import BaseModel

TransferTaskID = Union[str, int, UUID]


class TransferSubmitError(Exception):
    pass


class TaskState(str, Enum):
    active = "active"
    inactive = "inactive"
    done = "done"
    error = "error"


class TaskInfo(BaseModel):
    state: TaskState
    error_info: Dict[str, Any]


def all_absolute(*paths):
    for p in paths:
        if not Path(p).is_absolute():
            raise ValueError(f"{p} must be an absolute path")


def all_relative(*paths):
    for p in paths:
        if Path(p).is_absolute():
            raise ValueError(f"{p} must be a relative path")


class TransferInterface(ABC.ABC):
    @ABC.abstractstaticmethod
    def submit_task(
        src_loc: str,
        dest_loc: str,
        src_dir: Path,
        dest_dir: Path,
        transfers: List[Tuple[Path, Path]],
    ) -> TransferTaskID:
        raise NotImplementedError

    @ABC.abstractstaticmethod
    def poll_tasks(task_ids: List[TransferTaskID]) -> List[TaskInfo]:
        raise NotImplementedError
