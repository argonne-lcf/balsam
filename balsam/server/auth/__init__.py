from fastapi import APIRouter

from .token import user_from_token

auth_router = APIRouter()
__all__ = ["user_from_token", "auth_router"]
