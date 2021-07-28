import logging
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session, exc

from balsam.schemas import UserCreate, UserOut
from balsam.server.models import get_session
from balsam.server.models.crud import users

from .password_utils import verify_password
from .token import create_access_token, user_from_token

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/password")


def authenticate_user_password(db: Session, username: str, password: str) -> UserOut:
    try:
        user = users.get_user_by_username(db, username)
    except exc.NoResultFound:
        logger.info(f"DB does not contain a user with username {username}")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)

    if not user.hashed_password:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)

    if not verify_password(password, user.hashed_password):
        logger.info(f"User {username} entered a Bad password")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)

    return UserOut(id=user.id, username=user.username)


@router.post("/login")
def login(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_session)) -> Dict[str, Any]:
    username = form_data.username
    password = form_data.password

    user = authenticate_user_password(db, username, password)
    token, expiry = create_access_token(user)
    return {"access_token": token, "token_type": "bearer", "expiration": expiry}


@router.get("/me", response_model=UserOut)
def profile(user: UserOut = Depends(user_from_token)) -> UserOut:
    return user


@router.post("/register", response_model=UserOut, status_code=status.HTTP_201_CREATED)
def register(user: UserCreate, db: Session = Depends(get_session)) -> UserOut:
    if users.user_exists(db, user.username):
        raise HTTPException(status_code=400, detail="Username already taken")

    new_user = users.create_user(db, user.username, user.password)
    db.commit()
    return new_user
