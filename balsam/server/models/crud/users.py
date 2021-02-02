from typing import List

from sqlalchemy.orm import Session
from sqlalchemy.sql import exists

from balsam.schemas import UserOut
from balsam.server.auth.password import get_hash
from balsam.server.models.tables import User


def get_user_by_username(db: Session, username: str) -> User:
    return db.query(User).filter(User.username == username).one()


def user_exists(db: Session, username: str) -> bool:
    return bool(db.query(exists().where(User.username == username)).scalar())  # type: ignore


def create_user(db: Session, username: str, password: str) -> UserOut:
    hashed = get_hash(password)
    new_user = User(username=username, hashed_password=hashed)
    db.add(new_user)
    db.flush()
    return UserOut(id=new_user.id, username=new_user.username)


def list_users(db: Session) -> List[User]:
    return list(db.query(User))
