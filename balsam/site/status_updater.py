from collections import defaultdict
import multiprocessing
import queue
import signal
import logging
from datetime import datetime

from typing import List, Dict, Any, Optional
from balsam.schemas import JobState
from .service.util import Queue

logger = logging.getLogger(__name__)


class StatusUpdater(multiprocessing.Process):
    def __init__(self):
        super().__init__()
        self.queue = Queue()

    def run(self):
        EXIT_FLAG = False

        def handler(signum, stack):
            nonlocal EXIT_FLAG
            EXIT_FLAG = True

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

        while not EXIT_FLAG:
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
        logger.info(f"StatusUpdater thread finished.")

    def _drain_queue(self):
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
        state_data: Dict[str, Any] = None,
    ):
        self.queue.put_nowait(
            {
                "id": id,
                "state": state,
                "state_timestamp": state_timestamp,
                "state_data": state_data,
            }
        )

    def _perform_updates(self, updates: List[Dict]):
        raise NotImplementedError


class BulkStatusUpdater(StatusUpdater):
    def __init__(self, client):
        super().__init__()
        self.client = client

    def _perform_updates(self, updates: List[dict]):
        """
        In case two job updates occur in the same window,
        we sort updates by timestamp and avoid duplicate job
        updates in the same API call
        """
        updates_by_id = defaultdict(list)
        for update in sorted(updates, key=lambda x: x["state_timestamp"]):
            updates_by_id[update["id"]].append(update)

        while updates_by_id:
            bulk_update = [update_list.pop(0) for update_list in updates_by_id.values()]
            self.client.bulk_patch("jobs/", bulk_update)
            logger.info(f"StatusUpdater bulk-updated {len(bulk_update)} jobs")
            updates_by_id = {k: v for k, v in updates_by_id.items() if v}
