import os
import sys
from pydantic import BaseSettings
from datetime import timedelta
from importlib import import_module
import logging

logger = logging.getLogger(__name__)


class AuthSettings(BaseSettings):
    secret_key = "3f4abcaa16a006de8a7ef520a4c77c7d51b3ddb235c1ee2b99187f0f049eaa76"  # WARNING: Use real secret in production
    algorithm = "HS256"
    token_ttl: timedelta = timedelta(hours=12)
    auth_method: str = "balsam.server.auth.user_from_token"

    def get_auth_method(self):
        module, func = self.auth_method.rsplit(".", 1)
        return getattr(import_module(module), func)


class Settings(BaseSettings):
    class Config:
        env_prefix = "balsam_"
        case_sensitive = False
        env_file = ".env"

    database_url: str = "postgresql://postgres@localhost:5432/balsam"
    auth: AuthSettings = AuthSettings()
    redis_params: dict = {"unix_socket_path": "/tmp/redis-balsamapi.sock"}


if os.environ.get("BALSAM_TEST") or "test" in "".join(sys.argv):
    settings = Settings(_env_file=".env.test")
    logger.info(f"Loaded test settings:\n{settings}")
else:
    settings = Settings(_env_file=".env")
    logger.info(f"Loaded production settings:\n{settings}")
