import logging
from typing import TYPE_CHECKING, Any

from balsam.util import Process, SigHandler

if TYPE_CHECKING:
    from balsam.client import RESTClient

logger = logging.getLogger(__name__)


class BalsamService(Process):
    def __init__(self, client: "RESTClient", *args: Any, service_period: float = 1.0, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.client = client
        self.service_period = service_period

    def _run(self, *args: Any, **kwargs: Any) -> None:
        self.sig_handler = SigHandler()
        self.client.close_session()

        while not self.sig_handler.wait_until_exit(timeout=self.service_period):
            self.run_cycle()
        logger.info(f"Signal: {self.__class__.__name__} Process cleaning up")
        self.cleanup()
        logger.info(f"{self.__class__.__name__} Process exit")

    def run_cycle(self) -> None:
        pass

    def cleanup(self) -> None:
        pass
