import pytest
from pathlib import Path
import os
import subprocess
import socket
import time
from contextlib import closing

import balsam.server
from balsam.server import models
from balsam.server.models import crud
from balsam.client import BasicAuthRequestsClient
from .manager_base import Manager

BALSAM_TEST_DB = "postgresql://postgres@localhost:5432/balsam-test"


@pytest.fixture(scope="session")
def free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def setup_database():
    subprocess.run("dropdb -U postgres balsam-test", shell=True)
    subprocess.run("createdb -U postgres balsam-test", check=True, shell=True)

    models_dir = Path(__file__).parent.parent.joinpath("server/models")
    os.environ["balsam_database_url"] = BALSAM_TEST_DB
    balsam.server.settings.database_url = BALSAM_TEST_DB
    subprocess.run("alembic upgrade head", cwd=models_dir, check=True, shell=True)


@pytest.fixture(scope="session")
def live_server(setup_database, free_port):
    os.environ["balsam_database_url"] = BALSAM_TEST_DB
    proc = subprocess.Popen(
        f"uvicorn balsam.server.main:app --port {free_port}", shell=True,
    )
    time.sleep(1)
    yield f"http://localhost:{free_port}/"
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
    db_session.query(models.User).filter(models.User.id.in_(delete_ids)).delete(
        synchronize_session=False
    )
    db_session.commit()
    db_session.close()


@pytest.fixture(scope="function")
def client(create_user_client):
    client = create_user_client()
    Manager.set_client(client)
