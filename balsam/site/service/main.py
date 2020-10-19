import logging
import signal
from balsam.config import SiteConfig, ClientSettings

logger = logging.getLogger(__name__)


def main():
    config = SiteConfig()
    client = ClientSettings.load()
    services = []

    def handler(signum, stack):
        for service in services:
            service.sig_handler(signum, stack)

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    for cls, conf in config.get_service_classes():
        service = cls(client=client, **conf)
        service.start()
        services.append(service)

    for service in services:
        service.join()


if __name__ == "__main__":
    main()
