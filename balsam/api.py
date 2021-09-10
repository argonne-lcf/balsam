import logging

from balsam._api import models
from balsam._api.app import ApplicationDefinition
from balsam._api.models import App as _APIApp, BatchJob, EventLog, Job, Session, Site, TransferItem
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
    client = ClientSettings.load_from_file().build_client()
except Exception as exc:
    client = None  # type: ignore
    logger.warning(f"balsam.api failed to auto-load Client:\n{exc}")
else:
    Site.objects = models.SiteManager(client)
    _APIApp.objects = models.AppManager(client)
    BatchJob.objects = models.BatchJobManager(client)
    Job.objects = models.JobManager(client)
    TransferItem.objects = models.TransferItemManager(client)
    Session.objects = models.SessionManager(client)
    EventLog.objects = models.EventLogManager(client)
    ApplicationDefinition._set_client(client)
try:
    site_config = SiteConfig()
except Exception as exc:
    site_config = None  # type: ignore
    logger.debug(f"balsam.api failed to auto-load SiteConfig:\n{exc}")

__all__ = [
    "Site",
    "ApplicationDefinition",
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
