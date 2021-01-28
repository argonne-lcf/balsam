import os
import subprocess
from pathlib import Path

import pytest
from fastapi import status
from fastapi.testclient import TestClient

import balsam.server
from balsam.server import models
from balsam.server.main import app

from .util import BalsamTestClient


@pytest.fixture(scope="session")
def setup_database():
    subprocess.run("dropdb -U postgres balsam-test", shell=True)
    subprocess.run("createdb -U postgres balsam-test", check=True, shell=True)
    balsam.server.settings.database_url = "postgresql://postgres@localhost:5432/balsam-test"
    os.environ["balsam_database_url"] = balsam.server.settings.database_url

    models_dir = Path(__file__).parent.parent.joinpath("models")
    subprocess.run("alembic -x db=test upgrade head", cwd=models_dir, check=True, shell=True)


@pytest.fixture(scope="function")
def db_session():
    session = next(models.get_session())
    yield session
    session.close()


@pytest.fixture(scope="function")
def create_user_client(setup_database, db_session):
    db_session = next(models.get_session())
    idx = 0
    created_users = []

    def _create_user_client():
        nonlocal idx
        login_credentials = {"username": f"user{idx}", "password": "test-password"}
        idx += 1
        user = models.crud.users.create_user(db_session, **login_credentials)
        db_session.commit()
        created_users.append(user)

        client = BalsamTestClient(TestClient(app))
        data = client.post_form("/users/login", check=status.HTTP_200_OK, **login_credentials)
        token = data["access_token"]
        client.headers.update({"Authorization": f"Bearer {token}"})
        return client

    yield _create_user_client
    delete_ids = [user.id for user in created_users]
    db_session.query(models.User).filter(models.User.id.in_(delete_ids)).delete(synchronize_session=False)
    db_session.commit()


@pytest.fixture(scope="function")
def anon_client(setup_database):
    return BalsamTestClient(TestClient(app))


@pytest.fixture(scope="function")
def auth_client(create_user_client):
    return create_user_client()
