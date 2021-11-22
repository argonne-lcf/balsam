from typing import Callable, Dict, Iterator, List

from fastapi import APIRouter, Depends
from sqlalchemy import orm

from balsam import schemas
from balsam.server import settings
from balsam.server.conf import LoginMethod
from balsam.server.models import get_session

from . import authorization_code_login, device_code_login, password_login
from .token import user_from_token

LOGIN_ROUTERS: Dict[LoginMethod, APIRouter] = {
    LoginMethod.oauth_authcode: authorization_code_login.router,
    LoginMethod.oauth_device: device_code_login.router,
    LoginMethod.password: password_login.router,
}

AUTH_METHODS = {
    "user_from_token": user_from_token,
}


def get_auth_method() -> Callable[..., schemas.UserOut]:
    return AUTH_METHODS[settings.auth.auth_method]


def get_user_db_session(user: schemas.UserOut = Depends(get_auth_method())) -> Iterator[orm.Session]:
    return get_session(user)


def build_auth_router() -> APIRouter:
    """
    Enable login routes based on the configuration `settings.auth.login_methods`.
    Deployment config can thus enable/disable Oauth, Device flow, Password APIs
    """
    auth_router = APIRouter()
    for method in settings.auth.login_methods:
        auth_router.include_router(LOGIN_ROUTERS[method])

    @auth_router.get("/how")
    def get_login_methods() -> List[str]:
        methods = [str(s) for s in settings.auth.login_methods]
        return methods

    return auth_router


__all__ = [
    "user_from_token",
    "build_auth_router",
]
