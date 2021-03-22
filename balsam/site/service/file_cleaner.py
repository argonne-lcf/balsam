import itertools
import logging
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, DefaultDict, List

from balsam.schemas import JobState
from balsam.site import ApplicationDefinition

from .service_base import BalsamService

if TYPE_CHECKING:
    from balsam._api.models import Job  # noqa: F401
    from balsam.client import RESTClient

logger = logging.getLogger(__name__)


class FileCleanerService(BalsamService):
    def __init__(
        self,
        client: "RESTClient",
        site_id: int,
        apps_path: Path,
        data_path: Path,
        cleanup_batch_size: int = 180,  # cleanup after 180 jobs
        service_period: int = 30,  # cleanup every 30 seconds
    ) -> None:
        super().__init__(client=client, service_period=service_period)
        self.site_id = site_id
        self.data_path = data_path
        self.cleanup_batch_size = cleanup_batch_size
        self.app_cache = {
            app.id: ApplicationDefinition.load_app_class(apps_path, app.class_path)
            for app in self.client.App.objects.filter(site_id=self.site_id)
        }

    def remove_files(self, jobs: List["Job"], cleanup_file_patterns: List[str]) -> None:
        globs = itertools.chain(
            *(job.resolve_workdir(self.data_path).glob(pattern) for job in jobs for pattern in cleanup_file_patterns)
        )
        for match in globs:
            pstr = match.as_posix()
            if pstr.startswith(self.data_path.as_posix()) and ".." not in pstr:
                try:
                    match.unlink()
                    logger.info(f"Removed {pstr}")
                except (OSError, FileNotFoundError) as exc:
                    logger.warning(f"Cannot remove {pstr}: {exc}")

    def run_cycle(self) -> None:
        jobs_by_appid: DefaultDict[int, List["Job"]] = defaultdict(list)
        qs = self.client.Job.objects.filter(
            site_id=self.site_id, state=JobState.job_finished, pending_file_cleanup=True
        )
        for job in qs[: self.cleanup_batch_size]:
            jobs_by_appid[job.app_id].append(job)

        for appid, jobs in jobs_by_appid.items():
            cleanup_file_patterns = self.app_cache[appid].cleanup_files
            self.remove_files(jobs, cleanup_file_patterns)
            for job in jobs:
                job.pending_file_cleanup = False
            self.client.Job.objects.bulk_update(jobs)

    def cleanup(self) -> None:
        logger.info("Exiting FileCleaner service")
