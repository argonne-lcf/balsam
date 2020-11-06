import abc
from collections import defaultdict
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
    task_id: TransferTaskID
    state: TaskState
    info: Dict[str, Any]


def all_absolute(*paths):
    for p in paths:
        if not Path(p).is_absolute():
            raise ValueError(f"{p} must be an absolute path")


def all_relative(*paths):
    for p in paths:
        if Path(p).is_absolute():
            raise ValueError(f"{p} must be a relative path")


class TransferInterface(abc.ABC):
    _registry = {}

    @abc.abstractmethod
    def submit_task(
        self,
        remote_loc: str,
        direction: str,
        transfer_paths: List[Tuple[Path, Path, bool]],
    ) -> TransferTaskID:
        raise NotImplementedError

    @abc.abstractmethod
    def _poll_tasks(self, task_ids: List[TransferTaskID]) -> List[TaskInfo]:
        raise NotImplementedError

    @staticmethod
    def poll_tasks(task_ids: List[TransferTaskID]) -> List[TaskInfo]:
        ids_by_protocol = defaultdict(list)
        for task_id in task_ids:
            protocol, id = str(task_id).split(":")
            ids_by_protocol[protocol].append(id)

        result = []
        for protocol, id_list in ids_by_protocol.items():
            cls = TransferInterface._registry[protocol]
            task_infos = cls._poll_tasks(id_list)
            result.extend(task_infos)
        return result
