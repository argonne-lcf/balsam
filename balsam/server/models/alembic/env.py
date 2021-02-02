import logging

from alembic import context  # type: ignore

from balsam.server import settings
from balsam.server.models import Base, get_engine

logger = logging.getLogger("balsam.server.models.alembic.env")


metadata = Base.metadata
engine = get_engine()
logger.info(f"Alembic running migrations with DB engine: {engine}")

with engine.connect() as conn:
    context.configure(connection=conn, target_metadata=metadata, include_schemas=True)
    with context.begin_transaction():
        context.run_migrations()
