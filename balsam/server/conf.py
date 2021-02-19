import json
import logging
import os
from datetime import timedelta
from importlib import import_module
from typing import Any, Callable, Dict, List, Union

from pydantic import BaseSettings, validator

from balsam import schemas
from balsam.util import validate_log_level

logger = logging.getLogger(__name__)


class AuthSettings(BaseSettings):
    secret_key = (
        "3f4abcaa16a006de8a7ef520a4c77c7d51b3ddb235c1ee2b99187f0f049eaa76"  # WARNING: Use real secret in production
    )
    algorithm = "HS256"
    token_ttl: timedelta = timedelta(hours=48)
    auth_method: str = "balsam.server.auth.user_from_token"

    def get_auth_method(self) -> Callable[..., schemas.UserOut]:
        module, func = self.auth_method.rsplit(".", 1)
        return getattr(import_module(module), func)  # type: ignore


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
    num_uvicorn_workers: int = 1
    gunicorn_pid_file: str = "gunicorn.pid"

    @validator("log_level", always=True)
    def validate_balsam_log_level(cls, v: Union[str, int]) -> int:
        return validate_log_level(v)

    def gunicorn_env(self) -> List[str]:
        """
        Set os.environ and return args for Gunicorn
        """
        for k, v in json.loads(self.json()).items():
            envvar = (self.Config.env_prefix + k).upper()
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
                "-",
                "--error-logfile",
                "-",
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
