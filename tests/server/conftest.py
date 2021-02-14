from uuid import uuid4

import pytest
from fastapi import status
from fastapi.testclient import TestClient

from balsam.server import models

from .util import BalsamTestClient


@pytest.fixture(scope="function")
def fastapi_user_test_client(setup_database, db_session):
    db_session = next(models.get_session())
    created_users = []

    def _create_user_client():
        login_credentials = {"username": f"user{uuid4()}", "password": "test-password"}
        user = models.crud.users.create_user(db_session, **login_credentials)
        db_session.commit()
        created_users.append(user)

        from balsam.server.main import app

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
    from balsam.server.main import app

    return BalsamTestClient(TestClient(app))


@pytest.fixture(scope="function")
def auth_client(fastapi_user_test_client):
    return fastapi_user_test_client()
