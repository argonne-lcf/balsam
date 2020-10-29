import signal
import time
import logging
from balsam.util import Process

logger = logging.getLogger(__name__)


class BalsamService(Process):
    def __init__(self, client, *args, service_period=1.0, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = client
        self._EXIT_FLAG = False
        self._service_period = service_period

    def sig_handler(self, signum, stack):
        self._EXIT_FLAG = True

    def _run(self, *args, **kwargs):
        signal.signal(signal.SIGINT, self.sig_handler)
        signal.signal(signal.SIGTERM, self.sig_handler)
        self.client.close_session()
        while not self._EXIT_FLAG:
            self.run_cycle()
            time.sleep(self._service_period)
        logger.info(f"EXIT_FLAG: {self.__class__.__name__} Process cleaning up")
        self.cleanup()
        logger.info(f"{self.__class__.__name__} Process exit")

    def run_cycle(self):
        pass

    def cleanup(self):
        pass
