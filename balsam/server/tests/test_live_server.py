import pytest
import os
import time
import subprocess


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


@pytest.mark.django_db(transaction=True)
def test_concurrent_acquires_for_launch():
    pass
