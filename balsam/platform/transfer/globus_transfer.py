import os
from uuid import UUID
from .transfer import (
    TaskInfo,
    TransferInterface,
    TransferTaskID,
    TransferSubmitError,
    all_absolute,
    all_relative,
)
from typing import List, Tuple
from pathlib import Path
import subprocess
from globus_cli.services.transfer import get_client
from globus_sdk import TransferData


def submit_sdk(src_endpoint, dest_endpoint, batch):
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
    @staticmethod
    def _state_map(status):
        return {
            "active": "active",
            "inactive": "inactive",
            "succeeded": "done",
            "failed": "error",
        }[status.strip().lower()]

    @staticmethod
    def submit_task(
        src_loc: str,
        dest_loc: str,
        src_dir: Path,
        dest_dir: Path,
        transfers: List[Tuple[Path, Path, bool]],
    ) -> TransferTaskID:
        """Submit Transfer Task via Globus CLI"""
        # Validation
        src_endpoint = UUID(src_loc)
        dest_endpoint = UUID(dest_loc)
        all_absolute(src_dir, dest_dir)
        all_relative(*(path for transfer in transfers for path in transfer))

        transfers = [
            (src_dir.joinpath(src), dest_dir.joinpath(dest), recurse)
            for src, dest, recurse in transfers
        ]
        task_id = submit_sdk(src_endpoint, dest_endpoint, transfers)
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
            result.append(TaskInfo(task_id=d["task_id"], state=state, info=info,))
        return result


TransferInterface._registry["globus"] = GlobusTransferInterface
