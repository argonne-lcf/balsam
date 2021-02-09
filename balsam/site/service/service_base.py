import logging
import signal
import time
from typing import TYPE_CHECKING, Any

from balsam.util import Process

if TYPE_CHECKING:
    from balsam.client import RESTClient

logger = logging.getLogger(__name__)


class BalsamService(Process):
    def __init__(self, client: "RESTClient", *args: Any, service_period: float = 1.0, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.client = client
        self._EXIT_FLAG = False
        self._service_period = service_period

    def sig_handler(self, signum: Any, stack: Any) -> None:
        self._EXIT_FLAG = True

    def _run(self, *args: Any, **kwargs: Any) -> None:
        signal.signal(signal.SIGINT, self.sig_handler)
        signal.signal(signal.SIGTERM, self.sig_handler)
        self.client.close_session()
        while not self._EXIT_FLAG:
            self.run_cycle()
            time.sleep(self._service_period)
        logger.info(f"EXIT_FLAG: {self.__class__.__name__} Process cleaning up")
        self.cleanup()
        logger.info(f"{self.__class__.__name__} Process exit")

    def run_cycle(self) -> None:
        pass

    def cleanup(self) -> None:
        pass
