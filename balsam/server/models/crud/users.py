import logging
from datetime import datetime, timedelta
from typing import List, Optional, cast
from uuid import UUID

from sqlalchemy.orm import Session
from sqlalchemy.sql import exists

from balsam.schemas import UserOut
from balsam.server.auth.password_utils import get_hash
from balsam.server.models.tables import AuthorizationState, DeviceCodeAttempt, User

logger = logging.getLogger(__name__)


def get_user_by_username(db: Session, username: str) -> User:
    return db.query(User).filter(User.username == username).one()


def user_exists(db: Session, username: str) -> bool:
    return bool(db.query(exists().where(User.username == username)).scalar())  # type: ignore


def create_user(db: Session, username: str, password: Optional[str]) -> UserOut:
    if password:
        hashed = get_hash(password)
        new_user = User(username=username, hashed_password=hashed)
    else:
        new_user = User(username=username)
    db.add(new_user)
    db.flush()
    return UserOut(id=new_user.id, username=new_user.username)


def list_users(db: Session) -> List[User]:
    return list(db.query(User))


def create_device_code_attempt(
    db: Session,
    client_id: UUID,
    expiration: datetime,
    device_code: str,
    user_code: str,
    scope: str,
) -> None:
    attempt = DeviceCodeAttempt(
        client_id=cast(str, client_id),
        expiration=expiration,
        device_code=device_code,
        user_code=user_code,
        scope=scope,
    )
    db.add(attempt)
    db.flush()


def cleanup_device_code_attempts(db: Session) -> None:
    cleanup_before = datetime.utcnow() - timedelta(minutes=30)
    qs = db.query(DeviceCodeAttempt)
    to_clean = qs.filter((DeviceCodeAttempt.expiration <= cleanup_before) | DeviceCodeAttempt.user_denied)
    logger.debug(f"Deleting {to_clean.count()} expired/denied device-code attempts")
    to_clean.delete(synchronize_session=False)
    db.flush()


def get_device_code_attempt_by_client(db: Session, client_id: UUID, device_code: str) -> DeviceCodeAttempt:
    qs = db.query(DeviceCodeAttempt).filter(
        DeviceCodeAttempt.client_id == client_id,
        DeviceCodeAttempt.device_code == device_code,
    )
    return qs.one()


def get_device_code_attempt_by_user(db: Session, user_code: str) -> DeviceCodeAttempt:
    qs = db.query(DeviceCodeAttempt).filter(DeviceCodeAttempt.user_code == user_code)
    return qs.one()


def authorize_device_code_attempt(db: Session, user_code: str, user: UserOut) -> None:
    device_code_attempt = get_device_code_attempt_by_user(db, user_code)
    device_code_attempt.user_id = user.id
    db.flush()


def deny_device_code_attempt(db: Session, user_code: str) -> None:
    device_code_attempt = get_device_code_attempt_by_user(db, user_code)
    device_code_attempt.user_denied = True
    db.flush()


def delete_device_code_attempt(db: Session, device_code: str) -> None:
    qs = db.query(DeviceCodeAttempt).filter(DeviceCodeAttempt.device_code == device_code)
    qs.one()
    qs.delete(synchronize_session=False)
    db.flush()


def add_auth_state(db: Session, state: str) -> None:
    auth_state = AuthorizationState(state=state)
    db.add(auth_state)
    db.flush()


def verify_auth_state(db: Session, state: str) -> None:
    """
    Raises exception if there is no matching state in DB
    """
    qs = db.query(AuthorizationState).filter(AuthorizationState.state == state)
    qs.one()
    qs.delete(synchronize_session=False)
    db.flush()
