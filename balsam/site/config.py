import os
import sys
from pathlib import Path
import importlib.util
import yaml

from pydantic import (
    BaseSettings,
    PyObject,
    FilePath,
    PostgresDsn,
    AnyHttpUrl,
    validator,
    ValidationError,
)
from typing import Union
from balsam.client import RESTClient, RequestsClient


class ClientSettings(BaseSettings):
    client_class: PyObject
    address: Union[AnyHttpUrl, PostgresDsn] = "http://localhost:8000"
    username: str = ""
    password: str = ""
    database: str = "balsam"
    connect_timeout: float = 3.1
    read_timeout: float = 5.0
    retry_count: int = 3

    @validator("client_class")
    def client_type_is_correct(cls, v):
        if not issubclass(v, RESTClient):
            raise TypeError(f"client_class must subclass balsam.client.RESTClient")
        return v

    @validator("address")
    def address_matches_client_type(cls, v, values):
        client_class = values["client_class"]
        if issubclass(client_class, RequestsClient):
            assert v.startswith(
                "http"
            ), f"address must start with 'http' when using {client_class.__name__}"


class Settings(BaseSettings):
    credentials_file: FilePath = "~/.balsam/credentials"
    client: ClientSettings
    site_id: int


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
        self.site_path = self.resolve_site_path(site_path)

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

    def resolve_site_path(self, site_path):
        if site_path is not None:
            os.environ["BALSAM_SITE_PATH"] = site_path
        else:
            site_path = os.environ.get("BALSAM_SITE_PATH")
        if site_path is None:
            raise ValueError(
                "Initialize BalsamComponentFactory with a `site_path` or set env BALSAM_SITE_PATH "
                "to a Balsam site directory containing a settings.py file."
            )
        site_path = Path(site_path).resolve()
        if not site_path.is_dir():
            raise FileNotFoundError(
                f"BALSAM_SITE_PATH {site_path} must point to an existing Balsam site directory"
            )
        return site_path

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
    def _load_yaml_settings(file_path):
        with open(file_path) as fp:
            raw_settings = yaml.safe_load(fp)
        try:
            settings = Settings(**raw_settings)
        except ValidationError as exc:
            print(f"Please fix the issues in settings file: {file_path}")
            print(exc, file=sys.stderr)
            sys.exit(1)
        return settings
