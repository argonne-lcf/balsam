from collections import defaultdict
import multiprocessing
import queue
import signal
import logging
from datetime import datetime

from typing import List, Dict, Any, Optional
from balsam.schemas import JobState

Queue = multiprocessing.Queue
Queue().qsize()

logger = logging.getLogger(__name__)


class StatusUpdater(multiprocessing.Process):
    def __init__(self):
        super().__init__()
        self.queue = Queue()

    def run(self):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        while True:
            item = self.queue.get(block=True, timeout=None)
            if item == "exit":
                should_exit = True
                updates = []
            else:
                should_exit = False
                updates = [item]

            attempts = 0
            while attempts < 2:
                try:
                    item = self.queue.get(block=True, timeout=1)
                    if item != "exit":
                        updates.append(item)
                    else:
                        should_exit = True
                except queue.Empty:
                    attempts += 1
            if updates:
                self.perform_updates(updates)
            if should_exit:
                break

        self._on_exit()
        logger.info(f"StatusUpdater thread finished.")

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

    def set_exit(self):
        self.queue.put("exit")

    def perform_updates(self, updates: List[Dict]):
        raise NotImplementedError

    def _on_exit(self):
        pass


class BalsamStatusUpdater(StatusUpdater):
    def __init__(self, client):
        super().__init__()
        self.client = client

    def perform_updates(self, updates: List[dict]):
        updates_by_id = defaultdict(list)
        for update in sorted(updates, key=lambda x: x["state_timestamp"]):
            updates_by_id[update["id"]].append(update)

        while updates_by_id:
            bulk_update = [update_list.pop(0) for update_list in updates_by_id.values()]
            self.client.bulk_patch("jobs/", bulk_update)
            logger.info(f"StatusUpdater bulk-updated {len(bulk_update)} jobs")
            updates_by_id = {k: v for k, v in updates_by_id.items() if v}
