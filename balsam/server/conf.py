import json
import logging
import os
from datetime import timedelta
from importlib import import_module
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

from pydantic import BaseSettings, validator

from balsam import schemas
from balsam.util import validate_log_level

logger = logging.getLogger(__name__)


class OAuthProviderSettings(BaseSettings):
    class Config:
        env_prefix = "balsam_oauth_"
        case_sensitive = False
        env_file = ".env"
        extra = "forbid"

    # Environment must export BALSAM_OAUTH_CLIENT_ID, etc...
    client_id: str
    client_secret: str
    redirect_scheme: str = "https"
    redirect_path: str = "/auth/ALCF/callback"
    request_uri: str = "https://oauth2-dev.alcf.anl.gov/o/authorize/"
    token_uri: str = "https://oauth2-dev.alcf.anl.gov/o/token/"
    user_info_uri: str = "https://oauth2-dev.alcf.anl.gov/api/v1/user/"
    scope: str = "read"
    device_code_lifetime: timedelta = timedelta(seconds=300)
    device_poll_interval: timedelta = timedelta(seconds=3)


class AuthSettings(BaseSettings):
    class Config:
        env_prefix = "balsam_auth_"
        case_sensitive = False
        env_file = ".env"
        extra = "forbid"

    secret_key = (
        "3f4abcaa16a006de8a7ef520a4c77c7d51b3ddb235c1ee2b99187f0f049eaa76"  # WARNING: Use real secret in production
    )
    algorithm = "HS256"
    token_ttl: timedelta = timedelta(hours=48)
    auth_method: str = "balsam.server.auth.user_from_token"
    oauth_provider: Optional[OAuthProviderSettings] = None

    def get_auth_method(self) -> Callable[..., schemas.UserOut]:
        module, func = self.auth_method.rsplit(".", 1)
        return getattr(import_module(module), func)  # type: ignore

    @validator("oauth_provider", always=True)
    def get_oauth_config(cls, v: Optional[OAuthProviderSettings]) -> Optional[OAuthProviderSettings]:
        if v is None and os.environ.get("BALSAM_OAUTH_CLIENT_ID"):
            return OAuthProviderSettings()
        return v


class Settings(BaseSettings):
    class Config:
        env_prefix = "balsam_"
        case_sensitive = False
        env_file = ".env"
        extra = "forbid"

    server_bind: str = "0.0.0.0:8000"
    database_url: str = "postgresql://postgres@localhost:5432/balsam"
    auth: AuthSettings = AuthSettings()
    redis_params: Dict[str, Any] = {"unix_socket_path": "/tmp/redis-balsam.server.sock"}
    log_level: Union[str, int] = logging.INFO
    log_dir: Optional[Path]
    log_sql: bool = True
    num_uvicorn_workers: int = 1
    gunicorn_pid_file: str = "gunicorn.pid"

    @validator("log_level", always=True)
    def validate_balsam_log_level(cls, v: Union[str, int]) -> int:
        return validate_log_level(v)

    @validator("log_dir", always=True)
    def validate_log_dir(cls, v: Optional[Path]) -> Optional[Path]:
        if v is None:
            return None
        if not v.exists():
            v.mkdir(parents=True, exist_ok=False)
        elif not v.is_dir():
            raise ValueError(f"{v} is not a valid log directory")
        return v

    def gunicorn_env(self) -> List[str]:
        """
        Set os.environ and return args for Gunicorn
        """
        for k, v in json.loads(self.json()).items():
            envvar = (self.Config.env_prefix + k).upper()
            if v is not None:
                os.environ[envvar] = json.dumps(v) if not isinstance(v, str) else v

        # Settings from environ consistent with self
        assert Settings() == self
        env_str = " ".join(
            [
                "-k",
                "uvicorn.workers.UvicornWorker",
                "--bind",
                self.server_bind,
                "--log-level",
                str(self.log_level),
                "--access-logfile",
                self.log_dir.joinpath("gunicorn.access").as_posix() if self.log_dir else "/dev/null",
                "--error-logfile",
                self.log_dir.joinpath("gunicorn.out").as_posix() if self.log_dir else "/dev/null",
                "--capture-output",
                "--name",
                "balsam-server",
                "--workers",
                str(self.num_uvicorn_workers),
                "--pid",
                self.gunicorn_pid_file,
            ]
        )
        os.environ["GUNICORN_CMD_ARGS"] = env_str
        return ["gunicorn", "balsam.server.main:app"]
