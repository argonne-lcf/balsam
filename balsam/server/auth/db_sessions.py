from typing import Callable, Iterator

from fastapi import Depends
from sqlalchemy import orm

from balsam import schemas
from balsam.server import settings
from balsam.server.models import get_session

from .token import user_from_token

AUTH_METHODS = {
    "user_from_token": user_from_token,
}


def get_auth_method() -> Callable[..., schemas.UserOut]:
    return AUTH_METHODS[settings.auth.auth_method]


def get_admin_session() -> Iterator[orm.Session]:
    """
    Use with FastAPI Depends() to get a `postgres` superuser
    database connection.
    """
    session = get_session(user=None)
    try:
        yield session
    except:  # noqa: E722
        session.rollback()
        raise
    finally:
        session.close()


def get_webuser_session(user: schemas.UserOut = Depends(get_auth_method())) -> Iterator[orm.Session]:
    """
    Use with FastAPI Depends() to get a user-scoped database connection automatically from
    the request credentials.
    """
    session = get_session(user=user)
    try:
        yield session
    except:  # noqa: E722
        session.rollback()
        raise
    finally:
        session.close()
