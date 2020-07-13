from pathlib import Path
import yaml
from pydantic import BaseSettings
from datetime import timedelta
from importlib import import_module

SETTINGS_PATH = Path(__file__).parent.joinpath("settings.yml")


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
        env_prefix = "balsam"

    database_url: str = "postgresql://postgres@localhost:5432/balsam"
    auth: AuthSettings = AuthSettings()
    redis_params: dict = {"unix_socket_path": "/tmp/redis-balsamapi.sock"}

    @classmethod
    def from_yaml(cls):
        if not SETTINGS_PATH.exists():
            settings = cls()
            with open(SETTINGS_PATH, "w") as fp:
                yaml.dump(settings.dict(), fp)
        else:
            with open(SETTINGS_PATH) as fp:
                settings = cls(**yaml.load(fp, Loader=yaml.Loader))
        return settings
