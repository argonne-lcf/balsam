import logging

from sqlalchemy import create_engine, orm
from sqlalchemy.ext.declarative import declarative_base

import balsam.server

logger = logging.getLogger(__name__)

Base = declarative_base()
_engine = None
_Session = None


def get_engine():
    global _engine
    if _engine is None:
        logger.info(f"Creating DB engine: {balsam.server.settings.database_url}")
        _engine = create_engine(
            balsam.server.settings.database_url,
            connect_args={},
        )
    return _engine


def get_session():
    global _Session
    if _Session is None:
        _Session = orm.sessionmaker(bind=get_engine())

    session = _Session()
    try:
        yield session
    except:  # noqa: E722
        session.rollback()
        raise
    finally:
        session.close()


def create_tables():
    Base.metadata.create_all(get_engine())
