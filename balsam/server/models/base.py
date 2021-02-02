import logging
from typing import Iterator

from sqlalchemy import create_engine, orm
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base

import balsam.server

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
            connect_args={},
            pool_size=10,
            max_overflow=40,
        )
    return _engine


def get_session() -> Iterator[orm.Session]:
    global _Session
    if _Session is None:
        _Session = orm.sessionmaker(bind=get_engine())

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
