import logging
from typing import Optional

from sqlalchemy import create_engine, orm
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base

import balsam.server
from balsam.schemas.user import UserOut

logger = logging.getLogger(__name__)

Base = declarative_base()
_engine = None
_Session = None


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        logger.info(f"Creating DB engine: {balsam.server.settings.database_url}")
        _engine = create_engine(
            balsam.server.settings.database_url,
            pool_size=10,
            max_overflow=40,
            connect_args={"options": "-c timezone=utc"},
        )
    return _engine


def get_session(user: Optional[UserOut] = None) -> orm.Session:
    global _Session
    if _Session is None:
        _Session = orm.sessionmaker(bind=get_engine())

    session: orm.Session = _Session()
    return session


def create_tables() -> None:
    Base.metadata.create_all(get_engine())
