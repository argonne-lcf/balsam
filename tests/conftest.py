import os
import socket
import subprocess
import time
from contextlib import closing
from uuid import uuid4

import pytest
import requests

from balsam.client import BasicAuthRequestsClient
from balsam.server import models
from balsam.util import postgres as pg


@pytest.fixture(scope="session")
def setup_database():
    if os.environ.get("BALSAM_TEST_API_URL"):
        return
    env_url = os.environ.get("BALSAM_TEST_DB_URL", "postgresql://postgres@localhost:5432/balsam-test")
    pg.configure_balsam_server_from_dsn(env_url)
    try:
        session = next(models.get_session())
        if not session.engine.database.endswith("test"):
            raise RuntimeError("Database name used for testing must end with 'test'")
        session.execute("""TRUNCATE TABLE users CASCADE;""")
        session.commit()
        session.close()
    except Exception:
        pg.run_alembic_migrations(env_url)
    return env_url


@pytest.fixture(scope="session")
def free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def live_server(setup_database, free_port):
    default_url = os.environ.get("BALSAM_TEST_API_URL")
    if default_url:
        server_health_check(default_url, timeout=2.0, check_interval=0.5)
        yield default_url
        return

    os.environ["balsam_database_url"] = setup_database
    proc = subprocess.Popen(
        f"uvicorn balsam.server.main:app --port {free_port}",
        shell=True,
    )
    url = f"http://localhost:{free_port}/"
    server_health_check(url, timeout=10.0, check_interval=0.5)
    yield url
    proc.terminate()
    proc.communicate()
    return


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


def _make_user_client(url):
    login_credentials = {"username": f"user{uuid4()}", "password": "test-password"}
    requests.post(
        url.rstrip("/") + "/users/register",
        json=login_credentials,
    )
    client = BasicAuthRequestsClient(url, **login_credentials)
    client.refresh_auth()
    return client


@pytest.fixture(scope="function")
def create_user_client(live_server):
    created_clients = []

    def _create_user_client():
        client = _make_user_client(live_server)
        created_clients.append(client)
        return client

    yield _create_user_client
    for client in created_clients:
        for site in client.Site.objects.all():
            site.delete()


@pytest.fixture(scope="function")
def client(create_user_client):
    return create_user_client()


@pytest.fixture(scope="module")
def persistent_client(live_server):
    client = _make_user_client(live_server)
    # TODO: save client settings to temp file and set BALSAM_CREDENTIAL_PATH
    return client
