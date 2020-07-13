from sqlalchemy.orm.exc import NoResultFound
from fastapi import status, HTTPException
from passlib.context import CryptContext
from balsam import schemas
from balsam.server.models import crud


ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")


def verify_password(plain, hashed):
    return ctx.verify(plain, hashed)


def get_hash(password):
    return ctx.hash(password)


def authenticate_user_password(db, username, password):
    try:
        user = crud.users.get_user_by_username(db, username)
    except NoResultFound:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)

    if not verify_password(password, user.hashed_password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)

    return schemas.UserOut(id=user.id, username=user.username)
