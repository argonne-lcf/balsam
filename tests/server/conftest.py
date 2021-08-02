from uuid import uuid4

import pytest
from fastapi import status
from fastapi.testclient import TestClient

import balsam.server
from balsam.client import urls
from balsam.server import models
from balsam.server.models.crud import users

from .util import BalsamTestClient


@pytest.fixture(scope="function")
def db_session(setup_database):
    session = next(models.get_session())
    yield session
    session.close()


@pytest.fixture(scope="function")
def fastapi_user_test_client(setup_database, db_session):
    created_users = []

    def _client_factory():
        login_credentials = {"username": f"user{uuid4()}", "password": "test-password"}
        user = users.create_user(db_session, **login_credentials)
        db_session.commit()
        created_users.append(user)

        balsam.server.settings.auth.login_methods = ["password"]
        from balsam.server.main import app

        client = BalsamTestClient(TestClient(app))
        data = client.post_form(urls.PASSWORD_LOGIN, check=status.HTTP_200_OK, **login_credentials)
        token = data["access_token"]
        client.headers.update({"Authorization": f"Bearer {token}"})
        return client

    yield _client_factory
    delete_ids = [user.id for user in created_users]
    db_session.query(models.User).filter(models.User.id.in_(delete_ids)).delete(synchronize_session=False)
    db_session.commit()


@pytest.fixture(scope="function")
def anon_client(setup_database):
    from balsam.server.main import app

    return BalsamTestClient(TestClient(app))


@pytest.fixture(scope="function")
def auth_client(fastapi_user_test_client):
    return fastapi_user_test_client()
