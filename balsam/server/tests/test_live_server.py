import requests
from requests.auth import HTTPBasicAuth
import pytest
import os
import time
import subprocess
from balsam.server.models import User


@pytest.fixture(scope="module")
def gunicorn_server():
    """
    Run a live, multi-process server to test real clients & concurrency issues
    """
    host_port = os.environ.setdefault("GUNICORN_BIND", "127.0.0.1:8000")
    num_workers = os.environ.setdefault("GUNICORN_WORKERS", "4")
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "balsam.server.conf.settings")
    guni_args = [
        "gunicorn",
        "balsam.server.conf.wsgi",
        "--bind",
        host_port,
        "--workers",
        num_workers,
    ]

    gunicorn_master = subprocess.Popen(guni_args)
    time.sleep(1.0)
    yield "http://" + host_port
    gunicorn_master.terminate()
    gunicorn_master.wait()


@pytest.fixture(scope="function")
def requests_client(gunicorn_server, transactional_db):
    User.objects.create_user(username="user", email="user@aol.com", password="abc")
    session = requests.Session()
    session.auth = HTTPBasicAuth("misha", "f")
    response = session.post("http://127.0.0.1:8000/api/login")
    assert response.status_code == 200
    yield session
    session.close()


@pytest.mark.django_db(transaction=True)
def test_concurrent_acquires_for_launch(requests_client):
    pass
