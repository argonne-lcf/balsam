from sqlalchemy.orm import Session
from sqlalchemy.sql import exists
from balsam.server.models.tables import User
from balsam.schemas import UserOut
from balsam.server.auth.password import get_hash


def get_user_by_username(db: Session, username: str):
    return db.query(User).filter(User.username == username).one()


def user_exists(db: Session, username: str):
    return db.query(exists().where(User.username == username)).scalar()


def create_user(db: Session, username: str, password: str):
    hashed = get_hash(password)
    new_user = User(username=username, hashed_password=hashed)
    db.add(new_user)
    db.flush()
    return UserOut(id=new_user.id, username=new_user.username)


def list_users(db: Session):
    return list(db.query(User))
