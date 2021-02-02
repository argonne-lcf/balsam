import logging

from fastapi import HTTPException, status
from passlib.context import CryptContext  # type: ignore
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import NoResultFound

from balsam import schemas
from balsam.server.models import crud

logger = logging.getLogger(__name__)


ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")


def verify_password(plain: str, hashed: str) -> bool:
    return bool(ctx.verify(plain, hashed))


def get_hash(password: str) -> str:
    return str(ctx.hash(password))


def authenticate_user_password(db: Session, username: str, password: str) -> schemas.UserOut:
    try:
        user = crud.users.get_user_by_username(db, username)
    except NoResultFound:
        logger.info(f"DB does not contain a user with username {username}")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)

    if not user.hashed_password:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)

    if not verify_password(password, user.hashed_password):
        logger.info(f"User {username} entered a Bad password")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)

    return schemas.UserOut(id=user.id, username=user.username)
