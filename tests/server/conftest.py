import os

import pytest
from fastapi import status
from fastapi.testclient import TestClient

from balsam.server import models
from balsam.server.main import app
from balsam.util import postgres as pg

from .util import BalsamTestClient


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
