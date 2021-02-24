import logging
import os
import re
import secrets
import shutil
import socket
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import psycopg2  # type: ignore
import yaml

import balsam.server

SERVER_INFO_FILENAME = "server-info.yml"
MIN_VERSION = (10, 0, 0)
POSTGRES_MSG = f"""You need PostgreSQL {'.'.join(map(str, MIN_VERSION))} or newer.
Ensure pg_ctl is in the search PATH, and double-check version with pg_ctl --version.
You can easily grab the Postgres binaries at:
https://www.enterprisedb.com/download-postgresql-binaries
"""

logger = logging.getLogger(__name__)
OperationalError = psycopg2.OperationalError


def make_token() -> str:
    return secrets.token_urlsafe()


def make_token_file() -> Tuple[str, str]:
    with tempfile.NamedTemporaryFile(delete=False, mode="w", encoding="utf-8") as fp:
        token = make_token()
        fp.write(token)
        tmp_pwfile = fp.name
    return token, tmp_pwfile


def identify_free_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("", 0))
    port = int(sock.getsockname()[1])
    sock.close()
    return port


def identify_hostport() -> Tuple[str, int]:
    host = socket.gethostname()
    port = identify_free_port()
    return (host, port)


def write_pwfile(
    db_path: Union[Path, str],
    username: str,
    password: str,
    host: str,
    port: int,
    database: str,
    filename: str = SERVER_INFO_FILENAME,
) -> Dict[str, Any]:
    pw_dict = dict(username=username, password=password, host=host, port=port, database=database)
    pw_dict["scheme"] = "postgres"
    pw_dict["client_class"] = "balsam.client.DirectAPIClient"
    with open(Path(db_path).joinpath(filename), "w") as fp:
        yaml.dump(pw_dict, fp, sort_keys=False, indent=4)
    return pw_dict


def load_pwfile(db_path: Union[Path, str], filename: str = SERVER_INFO_FILENAME) -> Dict[str, Any]:
    with open(Path(db_path).joinpath(filename)) as fp:
        pw_dict = yaml.safe_load(fp)
    assert isinstance(pw_dict, dict)
    return pw_dict


def load_dsn(db_path: Union[Path, str], filename: str = SERVER_INFO_FILENAME) -> str:
    pw_dict = load_pwfile(db_path, filename)
    user = pw_dict["username"]
    pwd = pw_dict["password"]
    host = pw_dict["host"]
    port = pw_dict["port"]
    db = pw_dict["database"]
    return f"postgresql://{user}:{pwd}@{host}:{port}/{db}"


def create_new_db(db_path: Union[Path, str] = "balsamdb", database: str = "balsam") -> Dict[str, Any]:
    """
    Create & start a new PostgresDB cluster inside `db_path`
    A DB named `database` is created. If `pwfile` given, write DB credentials to
    `db_path/pwfile`.  Returns a dict containing credentials/connection
    info.
    """
    version_check()

    superuser, password = init_db_cluster(db_path)
    host, port = identify_hostport()
    mutate_conf_port(str(db_path), port)

    pw_dict = write_pwfile(
        db_path=db_path,
        username=superuser,
        password=password,
        host=host,
        port=port,
        database=database,
    )

    start_db(str(db_path))
    create_database(new_dbname=database, **pw_dict)
    return pw_dict


def configure_balsam_server(username: str, password: str, host: str, port: int, database: str, **kwargs: Any) -> str:
    dsn = f"postgresql://{username}:{password}@{host}:{port}/{database}"
    os.environ["balsam_database_url"] = dsn
    balsam.server.settings.database_url = dsn
    return dsn


def configure_balsam_server_from_dsn(dsn: str) -> None:
    os.environ["balsam_database_url"] = dsn
    os.environ["BALSAM_DATABASE_URL"] = dsn
    balsam.server.settings.database_url = dsn


def run_alembic_migrations(dsn: str, downgrade: Any = None) -> None:
    from alembic import command  # type: ignore
    from alembic.config import Config  # type: ignore

    import balsam.server.models.alembic as alembic

    migrations_path = str(Path(alembic.__file__).parent)

    logger.info(f"Running DB migrations in {migrations_path}")
    alembic_cfg = Config()
    alembic_cfg.set_main_option("script_location", str(migrations_path))
    # alembic_cfg.set_main_option("sqlalchemy.url", dsn)
    if downgrade is None:
        command.upgrade(alembic_cfg, "head")
    else:
        command.downgrade(alembic_cfg, downgrade)


# *******************************
# Functions that use a subprocess
# *******************************
def version_check() -> bool:
    p = subprocess.run(
        "pg_ctl --version",
        shell=True,
        encoding="utf-8",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=True,
    )
    stdout = p.stdout.strip()
    pattern = re.compile(r"(\d+\.)?(\d+\.)?(\*|\d+)")
    search_result = pattern.search(stdout)
    if not search_result:
        raise RuntimeError(f"Could not parse Postgres version from `pg_ctl --version`: {stdout}")
    match = search_result.group()
    major, minor, *rest = match.split(".")
    version_info: Tuple[int, ...]
    if rest and int(rest[0]) > 0:
        version_info = (int(major), int(minor), int(rest[0]))
    else:
        version_info = (int(major), int(minor))
    if version_info >= MIN_VERSION:
        return True
    raise RuntimeError(f"PostgreSQL {version_info} too old. {POSTGRES_MSG}")


def mutate_conf_port(db_path: Union[str, Path], port: int) -> None:
    settings_path = Path(db_path) / Path("postgresql.conf")
    if not settings_path.exists():
        raise ValueError(f"nonexistant {settings_path}")

    new_port = f"port={port}"
    cmd = f"sed -i -E 's/^[[:space:]]*port.*/{new_port}/g' {settings_path}"
    subprocess.run(cmd, shell=True, check=True)


def init_db_cluster(db_path: Union[Path, str], superuser: str = "postgres") -> Tuple[str, str]:
    db_path = Path(db_path)
    if db_path.exists():
        raise ValueError(f"{db_path} already exists")

    token, tmp_pwfile = make_token_file()

    subprocess.run(
        f"initdb -D {db_path} --auth scram-sha-256 -U {superuser} --pwfile {tmp_pwfile}",
        shell=True,
        check=True,
    )
    shutil.move(tmp_pwfile, db_path.joinpath("pwfile").as_posix())

    with open(db_path / "postgresql.conf", "a") as fp:
        fp.write("listen_addresses = '*' # appended from balsam init\n")
        fp.write("port=0 # appended from balsam init\n")
        fp.write("password_encryption = 'scram-sha-256'\n")
        fp.write("max_connections=210 # appended from balsam init\n")
        fp.write("shared_buffers=2GB # appended from balsam init\n")
        fp.write("synchronous_commit=off # appended from balsam init\n")
        fp.write("wal_writer_delay=400ms # appended from balsam init\n")
        fp.write("client_encoding = 'UTF8'\n")
        fp.write("timezone = 'UTC'\n")

        # logging
        fp.write("logging_collector=on # appended from balsam init\n")
        fp.write("log_min_duration_statement=0 # appended from balsam init\n")
        fp.write("log_connections=on # appended from balsam init\n")
        fp.write("log_duration=on # appended from balsam init\n")

    with open(db_path / "pg_hba.conf", "a") as fp:
        fp.write("host all all ::1/128 scram-sha-256\n")
        fp.write("host all all 0.0.0.0/0 scram-sha-256\n")
    return superuser, token


def start_db(db_path: Union[Path, str]) -> None:
    log_path = Path(db_path) / "postgres.log"
    start_cmd = f"pg_ctl -w start -D {db_path} -l {log_path} --mode=smart"
    logger.info(start_cmd)
    subprocess.run(start_cmd, shell=True, check=True)


def stop_db(db_path: Union[Path, str]) -> None:
    stop_cmd = f"pg_ctl -w stop -D {db_path} --mode=smart"
    res = subprocess.run(stop_cmd, shell=True)
    if res.returncode != 0:
        raise RuntimeError("Could not send stop signal; is the DB running on a different host?")


# ***********************************
# Functions that use a psycopg cursor
# ***********************************
class PgCursor:
    def __init__(
        self, host: str, port: int, username: str, password: str, database: str = "balsam", autocommit: bool = False
    ) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.autocommit = autocommit

    def __enter__(self) -> Any:
        self.conn = psycopg2.connect(
            dbname=self.database,
            user=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
        )
        self.conn.autocommit = self.autocommit
        self.cursor = self.conn.cursor()
        return self.cursor

    def __exit__(self, exc_type: Any, value: Any, traceback: Any) -> None:
        if not self.autocommit:
            if exc_type is None:
                self.conn.commit()
            else:
                self.conn.rollback()
        self.cursor.close()
        self.conn.close()


def create_database(new_dbname: str, host: str, port: int, username: str, password: str, **kwargs: Any) -> None:
    with PgCursor(host, port, username, password, database="", autocommit=True) as cur:
        cur.execute(
            f"""
        CREATE DATABASE {new_dbname};
        """
        )


def connections_list(
    host: str, port: int, username: str, password: str, database: str = "balsam", **kwargs: Any
) -> List[str]:
    with PgCursor(host, port, username, password) as cur:
        cur.execute(
            f"""
        SELECT pid,application_name,usename,state,substr(query, 1, 60) \
        FROM pg_stat_activity WHERE datname = '{database}';
        """
        )
        conns = cur.fetchall()
        return list(conns)


def create_user_and_pwfile(
    new_user: str,
    host: str,
    port: int,
    username: str,
    password: str,
    database: str = "balsam",
    pwfile: Optional[str] = None,
) -> None:
    """
    Create new Postgres user `new_user` and auto-generated
    token for the database at `host:port`.  A pwfile
    containing the DB credentials is written for transfer to the new user
    """
    token = make_token()
    pg_add_user(new_user, token, host, port, username, password, database)

    if pwfile is None:
        pwfile = f"{new_user}-server-info.yml"
    write_pwfile(
        db_path=".",
        username=new_user,
        password=token,
        host=host,
        port=port,
        database=database,
        filename=pwfile,
    )

    print(
        f"New Postgres user data in {pwfile}.\n"
        f"Protect this file in transit to {new_user}! "
        f"It contains the token necessary to reach the DB. "
        f"The new user can then reach the DB with "
        f"`balsam login --file {pwfile}`"
    )


def pg_add_user(
    new_user: str, new_password: str, host: str, port: int, username: str, password: str, database: str = "balsam"
) -> None:
    with PgCursor(host, port, username, password) as cur:
        cur.execute(
            f"""
        CREATE ROLE {new_user} WITH LOGIN PASSWORD '{new_password}'; \
        grant all privileges on database {database} to {new_user}; \
        GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {new_user}; \
        GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO {new_user};
        """
        )


def drop_user(
    deleting_user: str, host: str, port: int, username: str, password: str, database: str = "balsam", **kwargs: Any
) -> None:
    with PgCursor(host, port, username, password) as cur:
        cur.execute(
            f"""
        REVOKE ALL privileges on all tables in schema public FROM {deleting_user}; \
        REVOKE ALL privileges on all sequences in schema public FROM {deleting_user}; \
        REVOKE ALL privileges on database {database} FROM {deleting_user}; \
        DROP ROLE {deleting_user};
        """
        )


def list_users(host: str, port: int, username: str, password: str, **kwargs: Any) -> List[str]:
    with PgCursor(host, port, username, password) as cur:
        cur.execute("SELECT usename FROM pg_catalog.pg_user;")
        user_list = cur.fetchall()
        return list(user_list)
