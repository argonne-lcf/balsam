import logging
from collections import defaultdict
from itertools import islice
from math import ceil
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Tuple, cast
from urllib.parse import urlparse

from balsam.platform.transfer import TaskInfo, TransferInterface, TransferRetryableError, TransferSubmitError
from balsam.schemas import TransferItemState

from .service_base import BalsamService

if TYPE_CHECKING:
    from balsam._api.models import TransferItem, TransferItemQuery
    from balsam.client import RESTClient

logger = logging.getLogger(__name__)


def resolve_stage_in_paths(item: "TransferItem", workdir: Path) -> Tuple[Path, Path, bool]:
    src = item.remote_path
    dest = workdir.joinpath(item.local_path)
    if dest == workdir and not item.recursive:
        dest = dest.joinpath(src.name)
    if dest.parent != workdir:
        dest.parent.mkdir(parents=True, exist_ok=True)
    return src, dest, item.recursive


def resolve_stage_out_paths(item: "TransferItem", workdir: Path) -> Tuple[Path, Path, bool]:
    src = workdir.joinpath(item.local_path)
    dest = item.remote_path
    return src, dest, item.recursive


class TransferService(BalsamService):
    def __init__(
        self,
        client: "RESTClient",
        site_id: int,
        data_path: Path,
        transfer_interfaces: Dict[str, TransferInterface],
        transfer_locations: Dict[str, str],
        max_concurrent_transfers: int,
        transfer_batch_size: int,
        num_items_query_limit: int,
        service_period: int,
    ) -> None:
        super().__init__(client=client, service_period=service_period)
        self.site_id = site_id
        self.data_path = data_path
        self.transfer_locations = transfer_locations
        self.max_concurrent_transfers = max_concurrent_transfers
        self.transfer_interfaces = transfer_interfaces
        self.transfer_batch_size = transfer_batch_size
        self.num_items_query_limit = num_items_query_limit

    @staticmethod
    def build_task_map(transfer_items: Iterable["TransferItem"]) -> "defaultdict[str, List[TransferItem]]":
        task_map = defaultdict(list)
        for item in transfer_items:
            task_map[item.task_id].append(item)
        return task_map

    def update_transfers(self, transfer_items: List["TransferItem"], task: TaskInfo) -> None:
        current_state = cast(TransferItemState, task.state)
        if all(item.state == current_state for item in transfer_items):
            logger.debug(
                f"No change in {len(transfer_items)} TransferItems: "
                f"already in state {task.state} for task {task.task_id}"
            )
            return
        for item in transfer_items:
            item.state = current_state
            item.transfer_info = {**item.transfer_info, **task.info}
        self.client.TransferItem.objects.bulk_update(transfer_items)
        logger.info(f"Updated {len(transfer_items)} TransferItems " f"with task {task.task_id} to state {task.state}")

    @staticmethod
    def batch_iter(li: List[Any], bsize: int) -> Iterable[List[Any]]:
        nbatches = ceil(len(li) / bsize)
        for i in range(nbatches):
            yield li[i * bsize : (i + 1) * bsize]

    def item_batch_iter(self, items: "TransferItemQuery") -> Iterable[List["TransferItem"]]:
        transfer_candidates = defaultdict(list)
        for item in items[: self.num_items_query_limit]:
            transfer_candidates[(item.direction, item.location_alias)].append(item)
        task_keys = sorted(
            transfer_candidates.keys(),
            key=lambda k: len(transfer_candidates[k]),
            reverse=True,
        )
        for key in task_keys:
            key_items = transfer_candidates[key]
            for batch in self.batch_iter(key_items, bsize=self.transfer_batch_size):
                yield batch

    @staticmethod
    def error_items(items: List["TransferItem"], msg: str) -> None:
        logger.warning(msg)
        for item in items:
            item.state = TransferItemState.error
            item.transfer_info = {**item.transfer_info, "error": msg}

    def get_transfer_loc(self, loc_alias: str) -> Tuple[TransferInterface, str]:
        """Lookup trusted remote datastore and transfer method"""
        transfer_location = self.transfer_locations.get(loc_alias)
        if transfer_location is None:
            raise ValueError(f"Invalid location alias: {loc_alias}")
        parsed = urlparse(transfer_location)
        protocol = parsed.scheme
        remote_loc = parsed.netloc
        if protocol not in self.transfer_interfaces:
            raise ValueError(f"Unsupported Transfer protocol: {protocol}")
        transfer_interface = self.transfer_interfaces[protocol]
        return transfer_interface, remote_loc

    def get_workdirs_by_id(self, batch: List["TransferItem"]) -> Dict[int, Path]:
        """Build map of job workdirs, ensuring existence"""
        job_ids = list(set(item.job_id for item in batch))
        jobs = {job.id: job for job in self.client.Job.objects.filter(id=job_ids)}
        if len(jobs) < len(job_ids):
            raise RuntimeError("Could not find all Jobs for TransferItems")
        workdirs = {}
        for job_id, job in jobs.items():
            workdir = Path(self.data_path).joinpath(job.workdir).resolve()
            workdir.mkdir(parents=True, exist_ok=True)
            assert job_id is not None
            workdirs[job_id] = workdir
        return workdirs

    def submit_task(self, batch: List["TransferItem"]) -> None:
        loc_alias = batch[0].location_alias
        direction = batch[0].direction

        if direction not in ["in", "out"]:
            raise ValueError("direction must be in or out")
        if not all(item.location_alias == loc_alias for item in batch):
            raise ValueError("All items in batch must have same location_alias")
        if not all(item.direction == direction for item in batch):
            raise ValueError("All items in batch must have same direction")

        try:
            transfer_interface, remote_loc = self.get_transfer_loc(loc_alias)
        except ValueError as exc:
            return self.error_items(batch, str(exc))

        workdirs = self.get_workdirs_by_id(batch)
        path_resolver = resolve_stage_in_paths if direction == "in" else resolve_stage_out_paths
        transfer_paths = [path_resolver(item, workdirs[item.job_id]) for item in batch]

        try:
            task_id = transfer_interface.submit_task(remote_loc, direction, transfer_paths)
        except TransferSubmitError as exc:
            return self.error_items(batch, f"TransferSubmitError: {exc}")
        except TransferRetryableError as exc:
            logger.error(f"Non-fatal submit error: {exc}")
            return
        for item in batch:
            item.task_id = str(task_id)
            item.state = TransferItemState.active

    def run_cycle(self) -> None:
        TransferItem = self.client.TransferItem
        pending_submit = TransferItem.objects.filter(site_id=self.site_id, state={TransferItemState.pending})
        in_flight = TransferItem.objects.filter(
            site_id=self.site_id, state={TransferItemState.active, TransferItemState.inactive}
        )

        task_map = self.build_task_map(in_flight)
        task_ids: List[str] = list(task_map.keys())
        num_active_tasks = len(task_map)

        try:
            tasks = TransferInterface.poll_tasks(task_ids)
        except TransferRetryableError as exc:
            logger.error(f"Non-fatal error in poll_tasks: {exc}")
            return

        for task in tasks:
            items = task_map[task.task_id]
            self.update_transfers(items, task)

        max_new_tasks = max(0, self.max_concurrent_transfers - num_active_tasks)
        submit_batches = islice(self.item_batch_iter(pending_submit), max_new_tasks)
        for batch in submit_batches:
            if batch:
                self.submit_task(batch)
                TransferItem.objects.bulk_update(batch)

    def cleanup(self) -> None:
        pass
