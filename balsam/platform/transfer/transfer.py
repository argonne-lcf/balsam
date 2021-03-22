import abc
from collections import defaultdict
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple, Type

from pydantic import BaseModel


class TransferSubmitError(Exception):
    pass


class TransferRetryableError(Exception):
    pass


class TaskState(str, Enum):
    active = "active"
    inactive = "inactive"
    done = "done"
    error = "error"


class TaskInfo(BaseModel):
    task_id: str
    state: TaskState
    info: Dict[str, Any]


class TransferInterface(abc.ABC):
    _registry: Dict[str, Type["TransferInterface"]] = {}

    @abc.abstractmethod
    def submit_task(
        self,
        remote_loc: str,
        direction: str,
        transfer_paths: List[Tuple[Path, Path, bool]],
    ) -> str:
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    def _poll_tasks(task_ids: Sequence[str]) -> List[TaskInfo]:
        raise NotImplementedError

    @staticmethod
    def poll_tasks(task_ids: List[str]) -> List[TaskInfo]:
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
