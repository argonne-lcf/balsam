import os
from pathlib import Path
import importlib.util

from pydantic import BaseSettings, FilePath, PostgresDsn, HttpUrl, Union


class StaticHostedPostgresClient(BaseSettings):
    address: PostgresDsn


class AutoHostedPostgresClient(BaseSettings):
    address: PostgresDsn


class RequestsClient(BaseSettings):
    api_server: HttpUrl = "http://localhost:8000"
    api_version_root: str = "api"
    connect_timeout: float = 3.1
    read_timeout: float = 5.0
    retry_count: int = 3


class Settings(BaseSettings):
    credentials_file: FilePath = "~/.balsam/credentials"
    client: Union[StaticHostedPostgresClient, AutoHostedPostgresClient, RequestsClient]


class SiteConfiguration:
    def __init__(self, site_path=None):
        if site_path is not None:
            os.environ["BALSAM_SITE_PATH"] = site_path
        else:
            site_path = os.environ.get("BALSAM_SITE_PATH")

        if site_path is None:
            raise ValueError(
                "Initialize SiteConfiguration with a `site_path` or set env BALSAM_SITE_PATH "
                "to a Balsam site directory containing a settings.py file."
            )

        site_path = Path(site_path).resolve()
        if not site_path.is_dir():
            raise FileNotFoundError(
                f"BALSAM_SITE_PATH {site_path} must point to an existing Balsam site directory"
            )

        settings_path = site_path.joinpath("settings.py")
        if not settings_path.is_file():
            raise FileNotFoundError(
                f"BALSAM_SITE_PATH {site_path} must contain a settings.py file."
            )

        module = self._load_module(settings_path)
        settings = getattr(module, "BALSAM_SETTINGS", None)
        if settings is None:
            raise AttributeError(
                f"Your settings module {settings_path} is missing a BALSAM_SETTINGS object"
            )

        if not isinstance(settings, Settings):
            raise TypeError(
                f"The BALSAM_SETTINGS object in {settings_path} must be an instance of balsam.site.Settings"
            )

        self.site_path = site_path
        self.settings = settings

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

    def _load_module(self, file_path):
        spec = importlib.util.spec_from_file_location(file_path, file_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
