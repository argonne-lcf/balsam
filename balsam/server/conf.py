import logging
from datetime import timedelta
from importlib import import_module
from typing import Any, Callable, Dict, Union

from pydantic import BaseSettings, validator

from balsam import schemas
from balsam.util import validate_log_level

logger = logging.getLogger(__name__)


class AuthSettings(BaseSettings):
    secret_key = (
        "3f4abcaa16a006de8a7ef520a4c77c7d51b3ddb235c1ee2b99187f0f049eaa76"  # WARNING: Use real secret in production
    )
    algorithm = "HS256"
    token_ttl: timedelta = timedelta(hours=12)
    auth_method: str = "balsam.server.auth.user_from_token"

    def get_auth_method(self) -> Callable[..., schemas.UserOut]:
        module, func = self.auth_method.rsplit(".", 1)
        return getattr(import_module(module), func)  # type: ignore


class Settings(BaseSettings):
    class Config:
        env_prefix = "balsam_"
        case_sensitive = False
        env_file = ".env"

    database_url: str = "postgresql://postgres@localhost:5432/balsam"
    auth: AuthSettings = AuthSettings()
    redis_params: Dict[str, Any] = {"unix_socket_path": "/tmp/redis-balsam.server.sock"}
    balsam_log_level: Union[str, int] = logging.WARNING
    sqlalchemy_log_level: Union[str, int] = logging.WARNING

    @validator("balsam_log_level", always=True)
    def validate_balsam_log_level(cls, v: Union[str, int]) -> int:
        return validate_log_level(v)

    @validator("sqlalchemy_log_level", always=True)
    def validate_sqlalchemy_log_level(cls, v: Union[str, int]) -> int:
        return validate_log_level(v)


settings = Settings()
