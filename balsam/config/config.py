import os
import sys
from datetime import datetime
from pathlib import Path
import importlib.util
import socket
import shutil
from typing import Optional
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
from balsam.api.managers import (
    JobManager,
    SiteManager,
    AppManager,
    BatchJobManager,
    SessionManager,
    EventLogManager,
)


def balsam_home():
    return Path.home().joinpath(".balsam")


class ClientSettings(BaseSettings):
    client_class: PyObject
    host: str
    port: int
    username: str
    password: str
    scheme: str
    token: Optional[str] = None
    token_expiry: Optional[datetime] = None
    database: str = "balsam"
    api_root: str = "/api"
    connect_timeout: float = 3.1
    read_timeout: float = 5.0
    retry_count: int = 3

    @validator("client_class")
    def client_type_is_correct(cls, v):
        if not issubclass(v, RESTClient):
            raise TypeError(f"client_class must subclass balsam.client.RESTClient")
        return v

    @classmethod
    def from_url(cls, url):
        if url.scheme == "postgres":
            return cls(
                client_class="balsam.client.DirectAPIClient",
                host=url.host,
                port=url.port,
                username=url.user,
                password=url.password,
                scheme=url.scheme,
            )
        else:
            return cls(
                client_class="balsam.client.BasicAuthRequestsClient",
                host=url.host,
                port=url.port,
                username=url.user,
                password=url.password,
                api_root=url.path or "/api",
                scheme=url.scheme,
            )

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
        SiteManager(client.sites)
        AppManager(client.apps)
        JobManager(client.jobs)
        BatchJobManager(client.batch_jobs)
        SessionManager(client.sessions)
        EventLogManager(client.events)
        return client


class Settings(BaseSettings):
    site_id: int
    trusted_data_sources: List[AnyUrl] = []

    def save(self, path):
        with open(path, "w") as fp:
            yaml.dump(self.dict(), fp)

    @classmethod
    def load(cls, path):
        with open(path) as fp:
            raw_data = yaml.safe_load(fp)
        return cls(**raw_data)


class BalsamComponentFactory:
    """
    This class plays the role of Dependency Injector
        https://en.wikipedia.org/wiki/Dependency_injection
    Uses validated settings to build components and provide dependencies:
        - Client
        - Client <-- API (build Client first; provide Client to API)
        - Scheduler Interface
        - MPIRun Interface
        - NodeTracker Interface
        - JobTemplate (setting chooses template file)
        - Service modules:
            - Queue Submitter <-- SchedulerInterface
            - App Watchdog
            - Acquisition & Stage-in module
            - Transitions Module
    NO Component should refer to external settings or set its own dependencies
    Instead, this class builds and injects needed settings/dependencies at runtime
    """

    def __init__(self, site_path=None, settings=None):
        """
        Load Settings from BALSAM_SITE_PATH/settings.py or settings.yml
        """
        self.site_path = self.resolve_site_path(site_path, raise_exc=True)

        if settings is not None:
            if not isinstance(settings, Settings):
                raise ValueError(
                    f"If you're passing the settings kwarg, it must be an instance of balsam.config.Settings. "
                    "Otherwise, leave settings=None to auto-load the settings stored at BALSAM_SITE_PATH."
                )
            self.settings = settings
            return

        py_settings = self.site_path.joinpath("settings.py")
        py_settings = py_settings if py_settings.is_file() else None
        yaml_settings = self.site_path.joinpath("settings.yml")
        yaml_settings = yaml_settings if yaml_settings.is_file() else None

        if not (py_settings or yaml_settings):
            raise FileNotFoundError(
                f"{site_path} must contain a settings.py or settings.yml"
            )
        elif py_settings:
            self.settings = self._load_py_settings(py_settings)
        else:
            self.settings = self._load_yaml_settings(yaml_settings)

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

        client = ClientSettings.load_from_home().build_client()
        SiteManager(client.sites)

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
            or BalsamComponentFactory.search_site_dir()
        )
        if site_path is None:
            if raise_exc:
                raise ValueError(
                    "Initialize BalsamComponentFactory with a `site_path` or set env BALSAM_SITE_PATH "
                    "to a Balsam site directory containing a settings.py file."
                )
            return None

        site_path = Path(site_path).resolve()
        if not site_path.is_dir():
            raise FileNotFoundError(
                f"BALSAM_SITE_PATH {site_path} must point to an existing Balsam site directory"
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

    @staticmethod
    def _load_py_settings(file_path):
        strpath = str(file_path)
        spec = importlib.util.spec_from_file_location(strpath, strpath)
        module = importlib.util.module_from_spec(spec)

        try:
            spec.loader.exec_module(module)
        except ValidationError as exc:
            print(f"Please fix the issues in settings file: {file_path}")
            print(exc, file=sys.stderr)
            sys.exit(1)

        settings = getattr(module, "BALSAM_SETTINGS", None)
        if settings is None:
            raise AttributeError(
                f"Your settings module {file_path} is missing a BALSAM_SETTINGS object."
                f" Be sure to create an instance of balsam.config.Settings in this file, with the "
                "exact name BALSAM_SETTINGS."
            )
        if not isinstance(settings, Settings):
            raise TypeError(
                f"The BALSAM_SETTINGS object in {file_path} must be an instance of balsam.site.Settings"
            )
        return settings

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
