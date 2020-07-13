from sqlalchemy.orm import Session
from balsamapi.models.tables import User
from balsamapi.models.schemas import UserOut
from balsamapi.auth.password import get_hash


def get_user_by_username(db: Session, username: str):
    return db.query(User).filter(User.username == username).one()


def create_user(db: Session, username: str, password: str):
    hashed = get_hash(password)
    new_user = User(username=username, hashed_password=hashed)
    db.add(new_user)
    db.flush()
    return UserOut(id=new_user.id, username=new_user.username)
