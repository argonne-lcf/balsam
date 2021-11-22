import logging
from typing import Iterator

from sqlalchemy import create_engine, orm
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base
from fastapi import Depends
from balsam.server import settings

auth = settings.auth.get_auth_method()

import balsam.server
from balsam import schemas

logger = logging.getLogger(__name__)

Base = declarative_base()
_engine = None
_Session = None


def get_engine(user: schemas.UserOut) -> Engine:
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


def get_session(user: schemas.UserOut = Depends(auth)) -> Iterator[orm.Session]:
    global _Session
    if _Session is None:
        _Session = orm.sessionmaker(bind=get_engine(user))

    session: orm.Session = _Session()
    try:
        yield session
    except:  # noqa: E722
        session.rollback()
        raise
    finally:
        session.close()


def create_tables() -> None:
    Base.metadata.create_all(get_engine())
