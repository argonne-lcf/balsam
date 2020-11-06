from pathlib import Path
import os
import click

# NOTE: lazy-import balsam.util.postgres inside each CLI handler
# Because psycopg2 is slow to import and would bog down the entire CLI
# if it were imported here!


@click.group()
def db():
    """
    Setup or manage a local Postgres DB

    All subcommands require either:

        1) db-path as the first positional argument

        2) Environment variable BALSAM_DB_PATH
    """


@db.command()
@click.argument("db-path", type=click.Path(writable=True))
def init(db_path):
    """
    Setup & start a new Postgres DB

    Sets up the Balsam database and runs DB migrations
    The DB connection info is written to server-info.yml
    """
    from balsam.util import postgres

    db_path = Path(db_path)
    if db_path.exists():
        raise click.BadParameter(f"The path {db_path} already exists")

    pw_dict = postgres.create_new_db(db_path, database="balsam")
    dsn = postgres.configure_balsam_server(**pw_dict)
    postgres.run_alembic_migrations(dsn)


@db.command()
@click.argument("db-path", type=click.Path(exists=True))
@click.option("--downgrade", default=None)
def migrate(db_path, downgrade):
    """
    Update DB schema (run after upgrading Balsam version)
    """
    from balsam.util import postgres
    import balsam.server

    db_path = Path(db_path)
    try:
        pw_dict = postgres.load_pwfile(db_path)
    except FileNotFoundError:
        raise click.BadParameter(
            f"There is no {postgres.SERVER_INFO_FILENAME} in {db_path}"
        )

    user = pw_dict["username"]
    passwd = pw_dict["password"]
    host = pw_dict["host"]
    port = pw_dict["port"]
    database = pw_dict["database"]
    dsn = f"postgresql://{user}:{passwd}@{host}:{port}/{database}"
    os.environ["balsam_database_url"] = dsn
    balsam.server.settings.database_url = dsn

    click.echo("Running alembic migrations")
    postgres.run_alembic_migrations(dsn, downgrade=downgrade)
    click.echo("Migrations complete!")


@db.command()
@click.argument("db-path", envvar="BALSAM_DB_PATH", type=click.Path(exists=True))
def start(db_path):
    """
    Start a Postgres DB server locally, if not already running
    """
    from balsam.util import postgres, DirLock

    db_path = Path(db_path)

    # Avoid race condition when 2 scripts call balsam db start
    with DirLock(db_path, "db"):
        try:
            pw_dict = postgres.load_pwfile(db_path)
        except FileNotFoundError:
            raise click.BadParameter(
                f"There is no {postgres.SERVER_INFO_FILENAME} in {db_path}"
            )

        try:
            postgres.connections_list(**pw_dict)
        except postgres.OperationalError:
            click.echo("Cannot reach DB, will restart...")
        else:
            click.echo("DB is already running!")
            return

        host, port = postgres.identify_hostport()
        pw_dict.update(host=host, port=port)
        postgres.write_pwfile(db_path, **pw_dict)
        postgres.mutate_conf_port(db_path, port)
        postgres.start_db(db_path)


@db.command()
@click.argument("db-path", envvar="BALSAM_DB_PATH", type=click.Path(exists=True))
def stop(db_path):
    """
    Stop a local Postgres DB server process
    """
    from balsam.util import postgres, DirLock

    with DirLock(db_path, "db"):
        postgres.stop_db(db_path)


@db.command()
@click.argument("db-path", envvar="BALSAM_DB_PATH", type=click.Path(exists=True))
def connections(db_path):
    """
    List currently open database connections
    """
    from balsam.util import postgres

    try:
        pw_dict = postgres.load_pwfile(db_path)
    except FileNotFoundError:
        raise click.BadParameter(
            f"There is no {postgres.SERVER_INFO_FILENAME} in {db_path}"
        )

    connections = postgres.connections_list(**pw_dict)
    for conn in connections:
        click.echo(conn)


@db.command()
@click.argument("db-path", envvar="BALSAM_DB_PATH", type=click.Path(exists=True))
@click.argument("user")
def add_user(db_path, user):
    """
    Add a new authorized user to the DB

    Generates a {user}-server-info.yml for the user
    """
    from balsam.util import postgres

    try:
        pw_dict = postgres.load_pwfile(db_path)
    except FileNotFoundError:
        raise click.BadParameter(
            f"There is no {postgres.SERVER_INFO_FILENAME} in {db_path}"
        )

    postgres.create_user_and_pwfile(new_user=user, **pw_dict)


@db.command()
@click.argument("db-path", envvar="BALSAM_DB_PATH", type=click.Path(exists=True))
@click.argument("user")
def drop_user(db_path, user):
    """
    Remove a user from the DB
    """
    from balsam.util import postgres

    try:
        pw_dict = postgres.load_pwfile(db_path)
    except FileNotFoundError:
        raise click.BadParameter(
            f"There is no {postgres.SERVER_INFO_FILENAME} in {db_path}"
        )

    postgres.drop_user(deleting_user=user, **pw_dict)
    click.echo(f"Dropped user {user}")


@db.command()
@click.argument("db-path", envvar="BALSAM_DB_PATH", type=click.Path(exists=True))
def list_users(db_path):
    """
    List authorized DB users
    """
    from balsam.util import postgres

    try:
        pw_dict = postgres.load_pwfile(db_path)
    except FileNotFoundError:
        raise click.BadParameter(
            f"There is no {postgres.SERVER_INFO_FILENAME} in {db_path}"
        )

    user_list = postgres.list_users(**pw_dict)
    for user in user_list:
        click.echo(user)
