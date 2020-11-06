import os
from uuid import UUID
from .transfer import (
    TaskInfo,
    TransferInterface,
    TransferTaskID,
    TransferSubmitError,
)
from typing import List, Tuple
from pathlib import Path
import subprocess
from globus_cli.services.transfer import get_client
from globus_sdk import TransferData
import logging

logger = logging.getLogger(__name__)


def submit_sdk(src_endpoint: UUID, dest_endpoint: UUID, batch):
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
        verify_checksum=True,
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
    res = client.submit_transfer(transfer_data)
    task_id = res.get("task_id", None)
    if task_id is None:
        raise TransferSubmitError(str(res))
    logger.info(f"Submitted Globus transfer task {task_id}")
    return "globus:" + str(task_id)


def submit_subproc(src_endpoint, dest_endpoint, batch):
    env = os.environ.copy()
    env["LC_ALL"] = "C.UTF-8"
    env["LANG"] = "C.UTF-8"
    batch_str = "\n".join(
        f"{src} {dest} {'-r' if recurse else ''}" for src, dest, recurse in batch
    )
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
    return proc


class GlobusTransferInterface(TransferInterface):
    def __init__(self, endpoint_id: UUID):
        self.endpoint_id: UUID = UUID(str(endpoint_id))

    @staticmethod
    def _state_map(status):
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
        transfer_paths: List[Tuple[Path, Path, bool]],
    ) -> TransferTaskID:
        """Submit Transfer Task via Globus CLI"""
        if direction == "in":
            src_endpoint, dest_endpoint = UUID(str(remote_loc)), self.endpoint_id
        elif direction == "out":
            src_endpoint, dest_endpoint = self.endpoint_id, UUID(str(remote_loc))
        else:
            raise ValueError("direction must be in or out")
        task_id = submit_sdk(src_endpoint, dest_endpoint, transfer_paths)
        return task_id

    @staticmethod
    def _poll_tasks(task_ids: List[TransferTaskID]) -> List[TaskInfo]:
        client = get_client()
        filter_values = {"task_id": ",".join(map(str, task_ids))}
        filter_str = "/".join(f"{k}:{v}" for k, v in filter_values.items())
        result = []
        for d in client.task_list(num_results=None, filter=filter_str):
            state = GlobusTransferInterface._state_map(d["status"])
            info = {}
            if d.get("fatal_error"):
                info["error"] = d["fatal_error"]
            task_id = "globus:" + d["task_id"]
            result.append(TaskInfo(task_id=task_id, state=state, info=info))
        return result


TransferInterface._registry["globus"] = GlobusTransferInterface
