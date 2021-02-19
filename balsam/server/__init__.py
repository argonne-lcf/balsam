from fastapi import HTTPException, status

from .conf import Settings

__version__ = "0.1"


class ValidationError(HTTPException):
    def __init__(self, detail: str) -> None:
        super().__init__(status_code=status.HTTP_400_BAD_REQUEST, detail=detail)


settings = Settings()
__all__ = ["settings", "Settings", "ValidationError"]
