from .conf import settings
from fastapi import HTTPException, status

__version__ = "0.1"


class ValidationError(HTTPException):
    def __init__(self, detail):
        super().__init__(status_code=status.HTTP_400_BAD_REQUEST, detail=detail)


__all__ = ["settings", "ValidationError"]
