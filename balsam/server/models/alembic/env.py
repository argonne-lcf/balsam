from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context
from balsamapi.models import base

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = base.Base.metadata

# other values from the config, defined by the needs of env.py
cmd_kwargs = context.get_x_argument(as_dictionary=True)
if "db" not in cmd_kwargs:
    raise Exception(
        "We couldn't find `db` in the CLI arguments. "
        "Please verify `alembic` was run with `-x db=<db_name>` "
        "(e.g. `alembic -x db=development upgrade head`)"
    )
db_name = cmd_kwargs["db"]


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    alembic_config = config.get_section(config.config_ini_section)
    db_config = config.get_section(db_name)
    for key in db_config:
        alembic_config[key] = db_config[key]

    url = alembic_config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        include_schemas=True,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    alembic_config = config.get_section(config.config_ini_section)
    db_config = config.get_section(db_name)
    for key in db_config:
        alembic_config[key] = db_config[key]

    connectable = engine_from_config(
        alembic_config, prefix="sqlalchemy.", poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_schemas=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
