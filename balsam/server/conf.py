import logging
import os
from datetime import timedelta
from enum import Enum
from importlib import import_module
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

from pydantic import BaseSettings, validator

from balsam import schemas
from balsam.util import validate_log_level

logger = logging.getLogger(__name__)


class LoginMethod(str, Enum):
    oauth_authcode = "oauth_authcode"
    oauth_device = "oauth_device"
    password = "password"


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
    redirect_path: str = "/auth/oauth/ALCF/callback"
    request_uri: str = "https://oauth2-dev.alcf.anl.gov/o/authorize/"
    token_uri: str = "https://oauth2-dev.alcf.anl.gov/o/token/"
    user_info_uri: str = "https://oauth2-dev.alcf.anl.gov/api/v1/user/"
    scope: str = "read_basic_user_data"
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
    login_methods: List[LoginMethod] = [LoginMethod.password]
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

    database_url: str = "postgresql://postgres@localhost:5432/balsam"
    auth: AuthSettings = AuthSettings()
    redis_params: Dict[str, Any] = {"unix_socket_path": "/tmp/redis-balsam.server.sock"}
    log_level: Union[str, int] = logging.INFO
    log_dir: Optional[Path]

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

    def serialize_without_secrets(self) -> str:
        return self.json(
            exclude={
                "auth": {
                    "secret_key": ...,
                    "oauth_provider": {
                        "client_secret": ...,
                    },
                }
            }
        )
