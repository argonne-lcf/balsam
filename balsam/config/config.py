import os
import sys
from datetime import datetime
from pathlib import Path
import socket
import shutil
from typing import Optional, Dict
from uuid import UUID
import yaml

from pydantic import (
    BaseSettings,
    PyObject,
    AnyUrl,
    validator,
    ValidationError,
)
from typing import List
from balsam.client import RESTClient
from balsam.api import Site
from balsam.api import Manager
from balsam.schemas import AllowedQueue

from balsam.site.service import SchedulerService, ProcessingService
from balsam.util import config_logging


def balsam_home():
    return Path.home().joinpath(".balsam")


class ClientSettings(BaseSettings):
    api_root: str
    username: str
    client_class: PyObject = "balsam.client.BasicAuthRequestsClient"
    token: Optional[str] = None
    token_expiry: Optional[datetime] = None
    connect_timeout: float = 3.1
    read_timeout: float = 5.0
    retry_count: int = 3

    @validator("client_class")
    def client_type_is_correct(cls, v):
        if not issubclass(v, RESTClient):
            raise TypeError(f"client_class must subclass balsam.client.RESTClient")
        return v

    @staticmethod
    def settings_path():
        return balsam_home().joinpath("client.yml")

    @classmethod
    def load_from_home(cls):
        with open(cls.settings_path()) as fp:
            data = yaml.safe_load(fp)
        return cls(**data)

    def save_to_home(self):
        data = self.dict()
        cls = data["client_class"]
        data["client_class"] = cls.__module__ + "." + cls.__name__

        settings_path = self.settings_path()
        if settings_path.exists():
            os.chmod(settings_path, 0o600)
        else:
            open(settings_path, "w").close()
            os.chmod(settings_path, 0o600)
        with open(settings_path, "w+") as fp:
            yaml.dump(data, fp)

    def build_client(self):
        client = self.client_class(**self.dict())
        Manager.set_client(client)
        return client


class LoggingConfig(BaseSettings):
    level: str = "DEBUG"
    format: str = "%(asctime)s|%(process)d|%(thread)d|%(levelname)8s|%(name)s:%(lineno)s] %(message)s"
    datefmt: str = "%d-%b-%Y %H:%M:%S"
    buffer_num_records: int = 1024
    flush_period: int = 30


class SchedulerSettings(BaseSettings):
    scheduler_class: PyObject
    sync_period: int
    allowed_queues: Dict[str, AllowedQueue]
    allowed_projects: List[str] = []
    optional_batch_job_params = Dict[str, str]
    job_template_path: Path


class ProcessingSettings(BaseSettings):
    prefetch_depth: int
    filter_tags: Dict[str, str]
    num_workers: Dict[str, str]


class TransferInterfaceSettings(BaseSettings):
    trusted_locations: Dict[str, AnyUrl] = []
    max_concurrent_transfers: int
    globus_endpoint_id: UUID


class Settings(BaseSettings):
    site_id: int
    scheduler: SchedulerSettings
    processing: ProcessingSettings
    mpi_launcher: PyObject
    resource_manager: PyObject
    transfers: Optional[TransferInterfaceSettings]
    logging: LoggingConfig = LoggingConfig()

    def save(self, path):
        with open(path, "w") as fp:
            yaml.dump(self.dict(), fp)

    @classmethod
    def load(cls, path):
        with open(path) as fp:
            raw_data = yaml.safe_load(fp)
        return cls(**raw_data)


class SiteConfig:
    """
    Uses above settings to build components and provide dependencies
    No component should refer to external settings or set its own dependencies
    Instead, this class builds and injects needed settings/dependencies at runtime
    """

    def __init__(self, site_path=None, settings=None):
        self.site_path = self.resolve_site_path(site_path, raise_exc=True)

        if settings is not None:
            if not isinstance(settings, Settings):
                raise ValueError(
                    f"If you're passing the settings kwarg, it must be an instance of balsam.config.Settings. "
                    "Otherwise, leave settings=None to auto-load the settings stored at BALSAM_SITE_PATH."
                )
            self.settings = settings
            return

        yaml_settings = self.site_path.joinpath("settings.yml")

        if not yaml_settings.is_file():
            raise FileNotFoundError(f"{site_path} must contain a settings.yml")
        self.settings = self._load_yaml_settings(yaml_settings)

    def build_services(self):
        services = []
        client = ClientSettings.load_from_home().build_client()

        scheduler_service = SchedulerService(
            client=client,
            site_id=self.settings.site_id,
            submit_directory=self.job_path,
            **self.settings.scheduler.dict(),
        )
        services.append(scheduler_service)

        processing_service = ProcessingService(
            client=client,
            site_id=self.site_id,
            apps_path=self.apps_path,
            **self.settings.processing.dict(),
        )
        services.append(processing_service)
        return services

    @classmethod
    def new_site_setup(cls, site_path, hostname=None):
        """
        Creates a new site directory, registers Site
        with Balsam API, and writes default settings.yml into
        Site directory
        """
        site_path = Path(site_path)
        site_path.mkdir(exist_ok=True, parents=True)

        here = Path(__file__).parent
        with open(here.joinpath("default-site.yml")) as fp:
            default_site_data = yaml.safe_load(fp)
        default_site_path = here.joinpath(default_site_data["default_site_path"])

        settings = cls._load_yaml_settings(
            default_site_path.joinpath("settings.yml"), validate=False
        )

        ClientSettings.load_from_home().build_client()

        site = Site.objects.create(
            hostname=socket.gethostname() if hostname is None else hostname,
            path=site_path,
        )
        settings.site_id = site.pk
        settings.save(path=site_path.joinpath("settings.yml"))
        cf = cls(site_path=site_path, settings=settings)
        for path in [cf.log_path, cf.job_path, cf.data_path]:
            path.mkdir(exist_ok=True)

        shutil.copytree(
            src=default_site_path.joinpath("apps"), dst=cf.apps_path,
        )
        shutil.copy(
            src=default_site_path.joinpath("job-template.sh"), dst=cf.site_path,
        )
        cf.site_path.joinpath(".balsam-site").touch()

    @staticmethod
    def resolve_site_path(site_path=None, raise_exc=False):
        # Site determined from either passed argument, environ,
        # or walking up parent directories, in that order
        site_path = (
            site_path
            or os.environ.get("BALSAM_SITE_PATH")
            or SiteConfig.search_site_dir()
        )
        if site_path is None:
            if raise_exc:
                raise ValueError(
                    "Initialize SiteConfig with a `site_path` or set env BALSAM_SITE_PATH "
                    "to a Balsam site directory containing a settings.py file."
                )
            return None

        site_path = Path(site_path).resolve()
        if not site_path.is_dir():
            raise FileNotFoundError(
                f"BALSAM_SITE_PATH {site_path} must point to an existing Balsam site directory"
            )
        if not site_path.joinpath(".balsam-site").is_file():
            raise FileNotFoundError(
                f"BALSAM_SITE_PATH {site_path} is not a valid Balsam site directory "
                f"(does not contain a .balsam-site file)"
            )
        os.environ["BALSAM_SITE_PATH"] = str(site_path)
        return site_path

    @staticmethod
    def search_site_dir():
        check_dir = Path.cwd()
        while check_dir.as_posix() != "/":
            if check_dir.joinpath(".balsam-site").is_file():
                return check_dir
            check_dir = check_dir.parent

    def __getattr__(self, item):
        return getattr(self.settings, item)

    @property
    def apps_path(self):
        return self.site_path.joinpath("apps")

    @property
    def log_path(self):
        return self.site_path.joinpath("log")

    @property
    def job_path(self):
        return self.site_path.joinpath("qsubmit")

    @property
    def data_path(self):
        return self.site_path.joinpath("data")

    def enable_logging(self, basename, filename=None):
        if filename is None:
            ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
            filename = f"{basename}_{ts}.log"
        log_path = self.log_path.joinpath(filename)
        config_logging(
            filename=log_path, **self.settings.logging.dict(),
        )

    @staticmethod
    def _load_yaml_settings(file_path, validate=True):
        with open(file_path) as fp:
            raw_settings = yaml.safe_load(fp)
        if not validate:
            return Settings.construct(**raw_settings)
        try:
            settings = Settings(**raw_settings)
        except ValidationError as exc:
            print(f"Please fix the issues in settings file: {file_path}")
            print(exc, file=sys.stderr)
            sys.exit(1)
        return settings
