import logging
import logging.handlers
import sys
from pathlib import Path
from typing import Any, Optional, Union


def log_uncaught_exceptions(exctype: Any, value: Any, tb: Any) -> None:
    root_logger = logging.getLogger("balsam.server")
    root_logger.error(f"Uncaught Exception {exctype}: {value}", exc_info=(exctype, value, tb))


def setup_logging(log_dir: Optional[Path], log_level: Union[str, int]) -> None:
    logging.getLogger("balsam").handlers.clear()

    logger = logging.getLogger("balsam.server")
    logger.handlers.clear()
    format = "%(asctime)s.%(msecs)03d | %(process)d | %(levelname)s | %(name)s:%(lineno)s] %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(format, datefmt=datefmt)

    handler = (
        logging.handlers.RotatingFileHandler(
            filename=log_dir / "server-balsam.log",
            maxBytes=int(32 * 1e6),
            backupCount=3,
        )
        if log_dir
        else logging.StreamHandler()
    )
    handler.setFormatter(formatter)
    logger.setLevel(log_level)
    logger.addHandler(handler)
    sys.excepthook = log_uncaught_exceptions
