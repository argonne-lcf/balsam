import logging
import os
import subprocess
from pathlib import Path
from typing import List, Sequence, Tuple, Union
from uuid import UUID

from globus_sdk import TransferData  # type: ignore
from globus_sdk.exc import GlobusConnectionError, TransferAPIError  # type: ignore

from balsam.util.globus_auth import get_client  # type: ignore

from .transfer import TaskInfo, TransferInterface, TransferRetryableError, TransferSubmitError

logger = logging.getLogger(__name__)


PathLike = Union[str, Path]
SrcDestRecursive = Tuple[PathLike, PathLike, bool]


def submit_sdk(src_endpoint: UUID, dest_endpoint: UUID, batch: Sequence[SrcDestRecursive]) -> str:
    client = get_client()
    notify_opts = {
        "notify_on_succeeded": False,
        "notify_on_failed": False,
        "notify_on_inactive": False,
    }
    transfer_data = TransferData(
        client,
        src_endpoint,
        dest_endpoint,
        label="",
        sync_level="size",
        verify_checksum=False,
        preserve_timestamp=False,
        encrypt_data=True,
        submission_id=None,
        delete_destination_extra=False,
        deadline=None,
        skip_activation_check=False,
        **notify_opts,
    )
    for src, dest, recurse in batch:
        transfer_data.add_item(str(src), str(dest), recursive=recurse)
    try:
        res = client.submit_transfer(transfer_data)
    except TransferAPIError as exc:
        raise TransferSubmitError(str(exc))
    task_id = res.get("task_id", None)
    if task_id is None:
        raise TransferSubmitError(str(res))
    logger.info(f"Submitted Globus transfer task {task_id}")
    return "globus:" + str(task_id)


def submit_subproc(src_endpoint: UUID, dest_endpoint: UUID, batch: List[SrcDestRecursive]) -> str:
    env = os.environ.copy()
    env["LC_ALL"] = "C.UTF-8"
    env["LANG"] = "C.UTF-8"
    batch_str = "\n".join(f"{src} {dest} {'-r' if recurse else ''}" for src, dest, recurse in batch)
    proc = subprocess.run(
        [
            "globus",
            "transfer",
            "--encrypt",
            "--batch",
            str(src_endpoint),
            str(dest_endpoint),
        ],
        input=batch_str,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        encoding="utf-8",
        env=env,
        check=True,
    )
    for line in proc.stdout.split("\n"):
        if "Task ID" in line:
            return "globus:" + line.split()[-1]
    raise TransferSubmitError("Could not parse Task ID from submission")


class GlobusTransferInterface(TransferInterface):
    def __init__(self, endpoint_id: UUID):
        self.endpoint_id: UUID = UUID(str(endpoint_id))

    @staticmethod
    def _state_map(status: str) -> str:
        return {
            "active": "active",
            "inactive": "inactive",
            "succeeded": "done",
            "failed": "error",
        }[status.strip().lower()]

    def submit_task(
        self,
        remote_loc: str,
        direction: str,
        transfer_paths: Sequence[Tuple[PathLike, PathLike, bool]],
    ) -> str:
        """Submit Transfer Task via Globus CLI"""
        if direction == "in":
            src_endpoint, dest_endpoint = UUID(str(remote_loc)), self.endpoint_id
        elif direction == "out":
            src_endpoint, dest_endpoint = self.endpoint_id, UUID(str(remote_loc))
        else:
            raise ValueError("direction must be in or out")
        try:
            task_id = submit_sdk(src_endpoint, dest_endpoint, transfer_paths)
        except GlobusConnectionError as exc:
            raise TransferRetryableError(f"GlobusConnectionError in Transfer task submission: {exc}") from exc
        return str(task_id)

    @staticmethod
    def _poll_tasks(task_ids: Sequence[str]) -> List[TaskInfo]:
        client = get_client()
        filter_values = {"task_id": ",".join(map(str, task_ids))}
        filter_str = "/".join(f"{k}:{v}" for k, v in filter_values.items())
        try:
            task_list = list(client.task_list(num_results=None, filter=filter_str))
        except GlobusConnectionError as exc:
            raise TransferRetryableError(f"GlobusConnectionError in client.task_list: {exc}")
        result = []
        for d in task_list:
            state = GlobusTransferInterface._state_map(d["status"])
            info = {}
            if d.get("fatal_error"):
                info["error"] = d["fatal_error"]
            task_id = "globus:" + d["task_id"]
            result.append(TaskInfo(task_id=task_id, state=state, info=info))
        return result


TransferInterface._registry["globus"] = GlobusTransferInterface
