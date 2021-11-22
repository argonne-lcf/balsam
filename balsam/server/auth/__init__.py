from typing import Dict, List

from fastapi import APIRouter

from balsam.server import settings
from balsam.server.conf import LoginMethod

from . import authorization_code_login, device_code_login, password_login
from .db_sessions import get_admin_session, get_auth_method, get_webuser_session
from .token import user_from_token

LOGIN_ROUTERS: Dict[LoginMethod, APIRouter] = {
    LoginMethod.oauth_authcode: authorization_code_login.router,
    LoginMethod.oauth_device: device_code_login.router,
    LoginMethod.password: password_login.router,
}


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
    "get_auth_method",
    "get_admin_session",
    "get_webuser_session",
]
