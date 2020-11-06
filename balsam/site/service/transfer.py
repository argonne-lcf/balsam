# flake8: noqa
from itertools import islice
from pathlib import Path
from math import ceil
from urllib.parse import urlparse
from collections import defaultdict
from .service_base import BalsamService
import logging
from balsam.platform.transfer import TransferInterface, TransferSubmitError

logger = logging.getLogger(__name__)


class TransferService(BalsamService):
    def __init__(
        self,
        client,
        site_id,
        data_path,
        transfer_interfaces,
        transfer_locations,
        max_concurrent_transfers,
        transfer_batch_size,
        num_items_query_limit,
        service_period,
    ):
        super().__init__(client=client, service_period=service_period)
        self.site_id = site_id
        self.data_path = data_path
        self.transfer_locations = transfer_locations
        self.max_concurrent_transfers = max_concurrent_transfers
        self.transfer_interfaces = transfer_interfaces
        self.transfer_batch_size = transfer_batch_size
        self.num_items_query_limit = num_items_query_limit
        logger.info(f"Initialized TransferService:\n{self.__dict__}")

    @staticmethod
    def build_task_map(transfer_items):
        task_map = defaultdict(list)
        for item in transfer_items:
            task_map[item.task_id].append(item)
        return task_map

    def update_transfers(self, transfer_items, task):
        for item in transfer_items:
            item.state = task.state
            item.transfer_info = {**item.transfer_info, **task.info}
        self.client.TransferItem.objects.bulk_update(transfer_items)
        logger.info(
            f"Updated {len(transfer_items)} TransferItems "
            f"with task {task.task_id} to state {task.state}"
        )

    @staticmethod
    def batch_iter(li, bsize):
        nbatches = ceil(len(li) / bsize)
        for i in range(nbatches):
            yield li[i * bsize : (i + 1) * bsize]

    def item_batch_iter(self, items):
        transfer_candidates = defaultdict(list)
        for item in items[: self.num_items_query_limit]:
            transfer_candidates[(item.direction, item.location_alias)].append(item)
        task_keys = sorted(
            transfer_candidates.keys(),
            key=lambda k: len(transfer_candidates[k]),
            reverse=True,
        )
        for key in task_keys:
            items = transfer_candidates[key]
            for batch in self.batch_iter(items, bsize=self.transfer_batch_size):
                yield batch

    @staticmethod
    def error_items(items, msg):
        logger.warning(msg)
        for item in items:
            item.state = "error"
            item.transfer_info = {**item.transfer_info, "error": msg}

    def get_transfer_loc(self, loc_alias):
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

    def get_workdirs_by_id(self, batch):
        """Build map of job workdirs, ensuring existence"""
        job_ids = [item.job_id for item in batch]
        jobs = {job.id: job for job in self.client.Job.objects.filter(id=job_ids)}
        if len(jobs) < len(job_ids):
            raise RuntimeError(f"Could not find all Jobs for TransferItems")
        workdirs = {}
        for job_id, job in jobs.items():
            workdir = Path(self.data_path).joinpath(job.workdir).resolve()
            workdir.mkdir(parents=True, exist_ok=True)
            workdirs[job_id] = workdir
        return workdirs

    def submit_task(self, batch):
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
        if direction == "in":
            get_paths = lambda item: (
                item.remote_path,
                workdirs[item.job_id].joinpath(item.local_path),
            )
        else:
            get_paths = lambda item: (
                workdirs[item.job_id].joinpath(item.local_path),
                item.remote_path,
            )
        transfer_paths = [(*get_paths(item), item.recursive) for item in batch]

        try:
            task_id = transfer_interface.submit_task(
                remote_loc, direction, transfer_paths
            )
        except TransferSubmitError as exc:
            return self.error_items(batch, f"TransferSubmitError: {exc}")
        for item in batch:
            item.task_id = task_id
            item.state = "active"

    def run_cycle(self):
        TransferItem = self.client.TransferItem
        pending_submit = TransferItem.objects.filter(
            site_id=self.site_id, state=["pending"]
        )
        in_flight = TransferItem.objects.filter(
            site_id=self.site_id, state=["active", "inactive"]
        )

        task_map = self.build_task_map(in_flight)
        task_ids = list(task_map.keys())
        num_active_tasks = len(task_map)

        tasks = TransferInterface.poll_tasks(task_ids)
        for task in tasks:
            items = task_map[task.task_id]
            self.update_transfers(items, task)

        max_new_tasks = max(0, self.max_concurrent_transfers - num_active_tasks)
        submit_batches = islice(self.item_batch_iter(pending_submit), max_new_tasks)
        for batch in submit_batches:
            if batch:
                self.submit_task(batch)
                TransferItem.objects.bulk_update(batch)

    def cleanup(self):
        pass
