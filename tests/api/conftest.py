import os
import socket
import subprocess
import time
from contextlib import closing

import pytest
import requests

from balsam.client import BasicAuthRequestsClient
from balsam.server import models
from balsam.server.models import crud
from balsam.util import postgres as pg


@pytest.fixture(scope="session")
def free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def setup_database():
    env_url = os.environ.get("BALSAM_TEST_DB_URL", "postgresql://postgres@localhost:5432/balsam-test")
    pg.configure_balsam_server_from_dsn(env_url)
    try:
        session = next(models.get_session())
        session.execute("""TRUNCATE TABLE users CASCADE;""")
        session.commit()
        session.close()
    except Exception:
        pg.run_alembic_migrations(env_url)
    return env_url


def server_health_check(url, timeout=10, check_interval=0.5):
    conn_error = None
    for i in range(int(timeout / check_interval)):
        try:
            requests.get(url)
        except requests.ConnectionError as exc:
            time.sleep(check_interval)
            conn_error = str(exc)
        else:
            return True
    raise RuntimeError(conn_error)


@pytest.fixture(scope="session")
def live_server(setup_database, free_port):
    os.environ["balsam_database_url"] = setup_database
    proc = subprocess.Popen(
        f"uvicorn balsam.server.main:app --port {free_port}",
        shell=True,
    )
    url = f"http://localhost:{free_port}/"
    server_health_check(url)
    yield url
    proc.terminate()
    proc.communicate()


@pytest.fixture(scope="function")
def db_session():
    session = next(models.get_session())
    yield session
    session.close()


@pytest.fixture(scope="function")
def create_user_client(setup_database, db_session, live_server):
    db_session = next(models.get_session())
    idx = 0
    created_users = []

    def _create_user_client():
        nonlocal idx
        login_credentials = {"username": f"user{idx}", "password": "test-password"}
        idx += 1
        user = crud.users.create_user(db_session, **login_credentials)
        db_session.commit()
        created_users.append(user)
        client = BasicAuthRequestsClient(live_server, **login_credentials)
        client.refresh_auth()
        return client

    yield _create_user_client
    delete_ids = [user.id for user in created_users]
    db_session.query(models.User).filter(models.User.id.in_(delete_ids)).delete(synchronize_session=False)
    db_session.commit()
    db_session.close()


@pytest.fixture(scope="function")
def client(create_user_client):
    return create_user_client()
