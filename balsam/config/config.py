import json
import logging
import os
from abc import ABCMeta
from datetime import datetime
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Type, Union, cast
from uuid import UUID

import yaml
from pydantic import AnyUrl, BaseSettings, Field, ValidationError, validator

from balsam.client import NotAuthenticatedError, RequestsClient
from balsam.platform.app_run import AppRun
from balsam.platform.compute_node import ComputeNode
from balsam.platform.scheduler import SchedulerInterface
from balsam.platform.transfer import GlobusTransferInterface, TransferInterface
from balsam.schemas import AllowedQueue
from balsam.util import config_file_logging

if TYPE_CHECKING:
    from balsam._api.models import App  # noqa: F401
    from balsam.site.service.service_base import BalsamService

logger = logging.getLogger(__name__)


class InvalidSettings(Exception):
    pass


def balsam_home() -> Path:
    return Path.home().joinpath(".balsam")


def get_class_path(cls: type) -> str:
    return cls.__module__ + "." + cls.__name__


def import_string(dotted_path: str) -> Any:
    """
    Stolen from pydantic. Import a dotted module path and return the attribute/class designated by the
    last name in the path. Raise ImportError if the import fails.
    """
    try:
        module_path, class_name = dotted_path.strip(" ").rsplit(".", 1)
    except ValueError as e:
        raise ImportError(f'"{dotted_path}" doesn\'t look like a module path') from e

    module = import_module(module_path)
    try:
        return getattr(module, class_name)
    except AttributeError as e:
        raise ImportError(f'Module "{module_path}" does not define a "{class_name}" attribute') from e


class ClientSettings(BaseSettings):
    api_root: str
    client_class: Type[RequestsClient] = Field("balsam.client.BasicAuthRequestsClient")
    token: Optional[str] = None
    token_expiry: Optional[datetime] = None
    connect_timeout: float = 3.1
    read_timeout: float = 120.0
    retry_count: int = 3

    @validator("client_class", pre=True, always=True)
    def load_client_class(cls, v: str) -> Type[RequestsClient]:
        loaded = import_string(v) if isinstance(v, str) else v
        if not issubclass(loaded, RequestsClient):
            raise TypeError(f"client_class must subclass {get_class_path(RequestsClient)}")
        return cast(Type[RequestsClient], loaded)

    @staticmethod
    def settings_path() -> Path:
        env_path = os.environ.get("BALSAM_CLIENT_PATH")
        if env_path:
            return Path(env_path)
        else:
            return balsam_home().joinpath("client.yml")

    @classmethod
    def load_from_file(cls) -> "ClientSettings":
        try:
            with open(cls.settings_path()) as fp:
                data = yaml.safe_load(fp)
        except FileNotFoundError:
            raise NotAuthenticatedError(
                f"Client credentials {cls.settings_path()} do not exist. " f"Please authenticate with `balsam login`."
            )
        return cls(**data)

    def save_to_file(self) -> None:
        data = self.dict()
        cls = data["client_class"]
        data["client_class"] = get_class_path(cls)

        settings_path = self.settings_path()
        if not settings_path.parent.is_dir():
            settings_path.parent.mkdir()
        if settings_path.exists():
            os.chmod(settings_path, 0o600)
        else:
            open(settings_path, "w").close()
            os.chmod(settings_path, 0o600)
        with open(settings_path, "w+") as fp:
            yaml.dump(data, fp, sort_keys=False, indent=4)

    def build_client(self) -> RequestsClient:
        client = self.client_class(**self.dict(exclude={"client_class"}))
        return client


class LoggingConfig(BaseSettings):
    level: str = "DEBUG"
    format: str = "%(asctime)s.%(msecs)03d | %(process)d | %(levelname)s | %(name)s:%(lineno)s] %(message)s"
    datefmt: str = "%Y-%m-%d %H:%M:%S"
    buffer_num_records: int = 1024
    flush_period: int = 30


class SchedulerSettings(BaseSettings):
    scheduler_class: Type[SchedulerInterface] = Field("balsam.platform.scheduler.CobaltScheduler")
    sync_period: int = 60
    allowed_queues: Dict[str, AllowedQueue] = {
        "default": AllowedQueue(max_nodes=4010, max_walltime=24 * 60, max_queued_jobs=20),
        "debug-cache-quad": AllowedQueue(max_nodes=8, max_walltime=60, max_queued_jobs=1),
    }
    allowed_projects: List[str] = ["datascience", "magstructsADSP"]
    optional_batch_job_params: Dict[str, str] = {"singularity_prime_cache": "no"}
    job_template_path: Path = Path("job-template.sh")

    @validator("scheduler_class", pre=True, always=True)
    def load_scheduler_class(cls, v: str) -> Type[SchedulerInterface]:
        loaded = import_string(v) if isinstance(v, str) else v
        if not issubclass(loaded, SchedulerInterface):
            raise TypeError(f"scheduler_class must subclass {get_class_path(SchedulerInterface)}")
        return cast(Type[SchedulerInterface], loaded)


class QueueMaintainerSettings(BaseSettings):
    submit_period: int = 60
    submit_project: str = "local"
    submit_queue: str = "local"
    job_mode: str = "mpi"
    num_queued_jobs: int = 5
    num_nodes: int = 1
    wall_time_min: int = 1


class ElasticQueueSettings(BaseSettings):
    service_period: int = 60
    submit_project: str = "datascience"
    submit_queue: str = "balsam"
    job_mode: str = "mpi"
    min_wall_time_min: int = 35
    max_wall_time_min: int = 360
    wall_time_pad_min: int = 5
    min_num_nodes: int = 20
    max_num_nodes: int = 127
    max_queue_wait_time_min: int = 10
    max_queued_jobs: int = 20
    use_backfill: bool = True


class FileCleanerSettings(BaseSettings):
    cleanup_batch_size: int = 180
    service_period: int = 30


class ProcessingSettings(BaseSettings):
    num_workers: int = 5
    prefetch_depth: int = 1000


class TransferSettings(BaseSettings):
    transfer_locations: Dict[str, str] = {"theta_dtn": "globus://08925f04-569f-11e7-bef8-22000b9a448b"}
    max_concurrent_transfers: int = 5
    globus_endpoint_id: Optional[UUID] = None
    transfer_batch_size: int = 100
    num_items_query_limit: int = 2000
    service_period: int = 5


class LauncherSettings(BaseSettings):
    idle_ttl_sec: int = 10
    delay_sec: int = 1
    error_tail_num_lines: int = 10
    max_concurrent_mpiruns: int = 1000
    compute_node: Type[ComputeNode] = Field("balsam.platform.compute_node.ThetaKNLNode")
    mpi_app_launcher: Type[AppRun] = Field("balsam.platform.app_run.ThetaAprun")
    local_app_launcher: Type[AppRun] = Field("balsam.platform.app_run.LocalAppRun")
    mpirun_allows_node_packing: bool = False
    serial_mode_prefetch_per_rank: int = 64
    serial_mode_startup_params: Dict[str, str] = {"cpu_affinity": "none"}

    @validator("compute_node", pre=True, always=True)
    def load_compute_node_class(cls, v: str) -> Type[ComputeNode]:
        loaded = import_string(v) if isinstance(v, str) else v
        if not issubclass(loaded, ComputeNode):
            raise TypeError(f"compute_node must subclass {get_class_path(ComputeNode)}")
        return cast(Type[ComputeNode], loaded)

    @validator("mpi_app_launcher", pre=True, always=True)
    def load_mpi_app_launcher(cls, v: str) -> Type[AppRun]:
        loaded = import_string(v) if isinstance(v, str) else v
        if not issubclass(loaded, AppRun):
            raise TypeError(f"mpi_app_launcher must subclass {get_class_path(AppRun)}")
        return cast(Type[AppRun], loaded)

    @validator("local_app_launcher", pre=True, always=True)
    def load_local_app_launcher(cls, v: str) -> Type[AppRun]:
        loaded = import_string(v) if isinstance(v, str) else v
        if not issubclass(loaded, AppRun):
            raise TypeError(f"local_app_launcher must subclass {get_class_path(AppRun)}")
        return cast(Type[AppRun], loaded)


class Settings(BaseSettings):
    logging: LoggingConfig = LoggingConfig()
    filter_tags: Dict[str, str] = {"workflow": "test-1", "system": "H2O"}

    # Balsam service modules
    launcher: LauncherSettings = LauncherSettings()
    scheduler: Optional[SchedulerSettings] = SchedulerSettings()
    processing: Optional[ProcessingSettings] = ProcessingSettings()
    transfers: Optional[TransferSettings] = TransferSettings()
    file_cleaner: Optional[FileCleanerSettings] = FileCleanerSettings()
    queue_maintainer: Optional[QueueMaintainerSettings] = QueueMaintainerSettings()
    elastic_queue: Optional[ElasticQueueSettings] = ElasticQueueSettings()

    def save(self, path: Union[str, Path]) -> None:
        with open(path, "w") as fp:
            fp.write(self.dump_yaml())

    def dump_yaml(self) -> str:
        return cast(
            str,
            yaml.dump(
                json.loads(self.json()),
                sort_keys=False,
                indent=4,
            ),
        )

    @classmethod
    def load(cls, path: Union[str, Path]) -> "Settings":
        with open(path) as fp:
            raw_data = yaml.safe_load(fp)
        return cls(**raw_data)

    class Config:
        json_encoders = {
            type: get_class_path,
            ABCMeta: get_class_path,
        }


class SiteConfig:
    """
    Uses above settings to build components and provide dependencies
    No component should refer to external settings or set its own dependencies
    Instead, this class builds and injects needed settings/dependencies at runtime
    """

    def __init__(self, site_path: Union[str, Path, None] = None, settings: Optional[Settings] = None) -> None:
        site_path, site_id = self.resolve_site_path(site_path)
        self.site_path: Path = site_path
        self.site_id: int = site_id
        self.client = ClientSettings.load_from_file().build_client()

        if settings is not None:
            if not isinstance(settings, Settings):
                raise ValueError(
                    "If you're passing the settings kwarg, it must be an instance of balsam.config.Settings. "
                    "Otherwise, leave settings=None to auto-load the settings stored at BALSAM_SITE_PATH."
                )
            self.settings = settings
            return

        yaml_settings = self.site_path.joinpath("settings.yml")

        if not yaml_settings.is_file():
            raise FileNotFoundError(f"{site_path} must contain a settings.yml")
        try:
            self.settings = Settings.load(yaml_settings)
        except ValidationError as exc:
            raise InvalidSettings(f"{yaml_settings} is invalid:\n{exc}")

    def build_services(self) -> "List[BalsamService]":
        from balsam.site.service import (
            ElasticQueueService,
            FileCleanerService,
            ProcessingService,
            QueueMaintainerService,
            SchedulerService,
            TransferService,
        )

        services: List[BalsamService] = []

        if self.settings.scheduler:
            scheduler_service = SchedulerService(
                client=self.client,
                site_id=self.site_id,
                submit_directory=self.job_path,
                filter_tags=self.settings.filter_tags,
                **dict(self.settings.scheduler),  # does not convert sub-models to dicts
            )
            services.append(scheduler_service)

        if self.settings.queue_maintainer:
            queue_maintainer = QueueMaintainerService(
                client=self.client,
                site_id=self.site_id,
                filter_tags=self.settings.filter_tags,
                **dict(self.settings.queue_maintainer),  # does not convert sub-models to dicts
            )
            services.append(queue_maintainer)

        if self.settings.elastic_queue:
            elastic_queue = ElasticQueueService(
                client=self.client,
                site_id=self.site_id,
                filter_tags=self.settings.filter_tags,
                **dict(self.settings.elastic_queue),
            )
            services.append(elastic_queue)

        if self.settings.processing:
            processing_service = ProcessingService(
                client=self.client,
                site_id=self.site_id,
                data_path=self.data_path,
                apps_path=self.apps_path,
                filter_tags=self.settings.filter_tags,
                **dict(self.settings.processing),  # does not convert sub-models to dicts
            )
            services.append(cast("BalsamService", processing_service))

        if self.settings.transfers:
            transfer_settings = dict(self.settings.transfers)
            transfer_interfaces: Dict[str, TransferInterface] = {}
            endpoint_id = transfer_settings.pop("globus_endpoint_id")
            if endpoint_id:
                transfer_interfaces["globus"] = GlobusTransferInterface(endpoint_id)
            transfer_service = TransferService(
                client=self.client,
                site_id=self.site_id,
                data_path=self.data_path,
                transfer_interfaces=transfer_interfaces,
                **dict(transfer_settings),
            )
            services.append(transfer_service)

        if self.settings.file_cleaner:
            cleaner_service = FileCleanerService(
                client=self.client,
                site_id=self.site_id,
                apps_path=self.apps_path,
                data_path=self.data_path,
                **dict(self.settings.file_cleaner),
            )
            services.append(cleaner_service)

        return services

    @staticmethod
    def resolve_site_path(site_path: Union[None, str, Path] = None) -> Tuple[Path, int]:
        # Site determined from either passed argument, environ,
        # or walking up parent directories, in that order
        site_path = site_path or os.environ.get("BALSAM_SITE_PATH") or SiteConfig.search_site_dir()
        if site_path is None:
            raise ValueError(
                "Initialize SiteConfig with a `site_path` or set env BALSAM_SITE_PATH "
                "to a Balsam site directory containing a settings.py file."
            )

        site_path = Path(site_path).resolve()
        if not site_path.is_dir():
            raise FileNotFoundError(f"BALSAM_SITE_PATH {site_path} must point to an existing Balsam site directory")
        try:
            site_id = int(site_path.joinpath(".balsam-site").read_text())
        except FileNotFoundError:
            raise FileNotFoundError(
                f"BALSAM_SITE_PATH {site_path} is not a valid Balsam site directory "
                f"(does not contain a .balsam-site file)"
            )
        os.environ["BALSAM_SITE_PATH"] = str(site_path)
        return site_path, site_id

    @staticmethod
    def search_site_dir() -> Optional[Path]:
        check_dir = Path.cwd()
        while check_dir.as_posix() != "/":
            if check_dir.joinpath(".balsam-site").is_file():
                return check_dir
            check_dir = check_dir.parent
        return None

    @property
    def apps_path(self) -> Path:
        return self.site_path.joinpath("apps")

    @property
    def log_path(self) -> Path:
        return self.site_path.joinpath("log")

    @property
    def job_path(self) -> Path:
        return self.site_path.joinpath("qsubmit")

    @property
    def data_path(self) -> Path:
        return self.site_path.joinpath("data")

    def enable_logging(self, basename: str, filename: Optional[str] = None) -> Dict[str, Any]:
        if filename is None:
            ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
            filename = f"{basename}_{ts}.log"
        log_path = self.log_path.joinpath(filename)
        config_file_logging(
            filename=log_path,
            **self.settings.logging.dict(),
        )
        return {"filename": log_path, **self.settings.logging.dict()}

    def update_site_from_config(self) -> None:
        site = self.client.Site.objects.get(id=self.site_id)
        old_dict = site.display_dict()
        if self.settings.scheduler:
            site.allowed_projects = self.settings.scheduler.allowed_projects
            site.allowed_queues = self.settings.scheduler.allowed_queues
            site.optional_batch_job_params = self.settings.scheduler.optional_batch_job_params
        if self.settings.transfers:
            site.transfer_locations = cast(Dict[str, AnyUrl], self.settings.transfers.transfer_locations)
            site.globus_endpoint_id = self.settings.transfers.globus_endpoint_id

        new_dict = site.display_dict()
        diff = {k: (old_dict[k], new_dict[k]) for k in old_dict if old_dict[k] != new_dict[k]}
        if diff:
            site.save()
            diff_str = "\n".join(f"{k}={diff[k][0]} --> {diff[k][1]}" for k in diff)
            logger.info(f"Updated Site parameters:\n{diff_str}")

    def fetch_apps(self) -> Dict[str, "App"]:
        return {app.class_path: app for app in self.client.App.objects.filter(site_id=self.site_id)}
