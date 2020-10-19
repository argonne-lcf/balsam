import multiprocessing
import queue
import signal
import logging
import time
from balsam.api.models import Job

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

            waited = False
            while True:
                try:
                    item = self.queue.get(block=False)
                    if item != "exit":
                        updates.append(item)
                    else:
                        should_exit = True
                except queue.Empty:
                    if waited:
                        break
                    else:
                        time.sleep(1.0)
                        waited = True
            if updates:
                self.perform_updates(updates)
            if should_exit:
                break

        self._on_exit()
        logger.info(f"StatusUpdater thread finished.")

    def set_exit(self):
        self.queue.put("exit")

    def perform_updates(self, updates):
        raise NotImplementedError

    def _on_exit(self):
        pass


class BalsamStatusUpdater(StatusUpdater):
    def perform_updates(self, updated_jobs):
        Job.objects.bulk_update(updated_jobs)
        logger.info(f"StatusUpdater bulk-updated {len(updated_jobs)} jobs")
