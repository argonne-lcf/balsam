from .conf import Settings as _SettingsCls
from fastapi import HTTPException, status

__version__ = "0.1"


class ValidationError(HTTPException):
    def __init__(self, detail):
        super().__init__(status_code=status.HTTP_400_BAD_REQUEST, detail=detail)


class _LazySettings:
    def __init__(self):
        self._settings = None

    @property
    def settings(self):
        if self._settings is None:
            self._settings = _SettingsCls.from_yaml()
        return self._settings

    def __getattr__(self, key):
        if key == "_settings":
            return super().__getattr__(key)
        return getattr(self.settings, key)

    def __setattr__(self, key, value):
        if key == "_settings":
            return super().__setattr__(key, value)
        return setattr(self.settings, key, value)


settings = _LazySettings()  # noqa
__all__ = ["settings"]
