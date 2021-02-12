import logging

from balsam.config import ClientSettings, SiteConfig
from balsam.schemas import (
    AppParameter,
    BatchJobPartition,
    JobMode,
    JobState,
    JobTransferItem,
    TransferDirection,
    TransferSlot,
)

logger = logging.getLogger(__name__)

try:
    client = ClientSettings.load_from_home().build_client()
except Exception as exc:
    client = None  # type: ignore
    logger.debug(f"balsam.api failed to auto-load Client:\n{exc}")
else:
    Site = client.Site
    App = client.App
    BatchJob = client.BatchJob
    Job = client.Job
    Session = client.Session
    TransferItem = client.TransferItem
    EventLog = client.EventLog

try:
    site_config = SiteConfig()
except Exception as exc:
    site_config = None  # type: ignore
    logger.debug(f"balsam.api failed to auto-load SiteConfig:\n{exc}")

__all__ = [
    "Site",
    "App",
    "BatchJob",
    "Job",
    "Session",
    "TransferItem",
    "EventLog",
    "JobTransferItem",
    "JobState",
    "AppParameter",
    "TransferDirection",
    "TransferSlot",
    "JobMode",
    "BatchJobPartition",
    "site_config",
]
