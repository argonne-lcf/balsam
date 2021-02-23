import logging
import queue
from collections import defaultdict
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional, cast

from balsam.schemas import JobState
from balsam.util import Process, SigHandler

from .util import Queue

if TYPE_CHECKING:
    from balsam.client import RESTClient

logger = logging.getLogger(__name__)


class StatusUpdater(Process):
    def __init__(self, client: "RESTClient") -> None:
        super().__init__()
        self.client = client
        self.queue: "Queue[Dict[str, Any]]" = Queue()

    def _run(self) -> None:
        self.client.close_session()
        sig_handler = SigHandler()

        while not sig_handler.is_set():
            try:
                item = self.queue.get(block=True, timeout=1)
            except queue.Empty:
                continue

            updates = [item]
            while len(updates) < 10_000:
                try:
                    item = self.queue.get(block=True, timeout=1)
                    updates.append(item)
                except queue.Empty:
                    break
            self._perform_updates(updates)

        logger.info("Signal: break out of StatusUpdater main loop")
        self._drain_queue()
        logger.info("StatusUpdater thread finished.")

    def _drain_queue(self) -> None:
        updates = []
        while True:
            try:
                item = self.queue.get_nowait()
                updates.append(item)
            except queue.Empty:
                break
        if updates:
            self._perform_updates(updates)

    def put(
        self,
        id: int,
        state: JobState,
        state_timestamp: Optional[datetime] = None,
        state_data: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        if state_data is None:
            state_data = {}
        self.queue.put_nowait(
            {
                "id": id,
                "state": state,
                "state_timestamp": state_timestamp,
                "state_data": state_data,
                **kwargs,
            }
        )

    def _perform_updates(self, updates: List[Dict[str, Any]]) -> None:
        raise NotImplementedError


class BulkStatusUpdater(StatusUpdater):
    def _perform_updates(self, updates: List[Dict[str, Any]]) -> None:
        """
        In case two job updates occur in the same window,
        we sort updates by timestamp and avoid duplicate job
        updates in the same API call
        """
        updates_by_id = cast(Dict[Any, List[Dict[str, Any]]], defaultdict(list))
        for update in sorted(updates, key=lambda x: x["state_timestamp"]):  # type: ignore
            updates_by_id[update["id"]].append(update)

        while updates_by_id:
            bulk_update = [update_list.pop(0) for update_list in updates_by_id.values()]
            self.client.bulk_patch("jobs/", bulk_update)
            logger.info(f"StatusUpdater bulk-updated {len(bulk_update)} jobs")
            updates_by_id = {k: v for k, v in updates_by_id.items() if v}
