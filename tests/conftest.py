import json
import os
import shutil
import socket
import subprocess
import tempfile
import time
from contextlib import closing
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional
from uuid import uuid4

import psutil  # type: ignore
import pytest
import requests

import balsam.server
from balsam.client import BasicAuthRequestsClient, urls
from balsam.cmdline.utils import start_site
from balsam.config import ClientSettings, SiteConfig, balsam_home, site_builder
from balsam.server import models
from balsam.site.app import sync_apps
from balsam.util import postgres as pg

from .test_platform import PLATFORMS, get_platform, get_test_api_url, get_test_db_url, get_test_dir, get_test_log_dir


def pytest_runtest_setup(item: Any) -> None:
    """
    PyTest calls this hook before each test.
    To mark a test for running only on Theta, use
    @pytest.mark.alcf_theta
    """
    supported_platforms = PLATFORMS.intersection(mark.name for mark in item.iter_markers())
    plat = get_platform()
    if supported_platforms and plat not in supported_platforms:
        pytest.skip("cannot run on platform {}".format(plat))


@pytest.fixture(scope="session")
def setup_database() -> Optional[str]:
    """
    If `BALSAM_TEST_API_URL` is exported do nothing: the database is managed elsewhere.
    Otherwise, configure the Test DB and wipe it clean.
    """
    if get_test_api_url():
        return None
    env_url = get_test_db_url()
    pg.configure_balsam_server_from_dsn(env_url)
    try:
        session = next(models.get_session())
        if not session.engine.database.endswith("test"):  # type: ignore
            raise RuntimeError("Database name used for testing must end with 'test'")
        pg.run_alembic_migrations(env_url)
        session.execute("""TRUNCATE TABLE users CASCADE;""")
        session.commit()
        session.close()
    except Exception as exc:
        print(f"Running migrations because could not flush `Users`:\n{exc}")
        pg.run_alembic_migrations(env_url)
    return env_url


@pytest.fixture(scope="session")
def free_port() -> str:
    """Returns a free port for the test server to bind"""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return str(s.getsockname()[1])


@pytest.fixture(scope="session")
def test_log_dir() -> Path:
    """
    Log directory (persists as artifact after test session)
    Used by Live server (gunicorn).
    At the end of a Site integration test, the Site logs are copied
    into this directory as well.
    """
    test_log_dir = get_test_log_dir()
    if test_log_dir.is_dir():
        shutil.rmtree(test_log_dir)
    test_log_dir.mkdir(exist_ok=False)
    return test_log_dir


@pytest.fixture(scope="session")
def live_server(setup_database: Optional[str], free_port: str, test_log_dir: Path) -> Iterable[str]:
    """
    If `BALSAM_TEST_API_URL` is exported, do a quick liveness check and return URL.
    Otherwise, startup Uvicorn test server and return URL after a liveness check.
    """
    default_url = get_test_api_url()
    if default_url:
        _server_health_check(default_url, timeout=2.0, check_interval=0.5)
        yield default_url
        return

    assert setup_database is not None

    os.environ["BALSAM_LOG_DIR"] = test_log_dir.as_posix()
    os.environ["BALSAM_LOG_LEVEL"] = "DEBUG"
    os.environ["BALSAM_AUTH_LOGIN_METHODS"] = json.dumps(["password"])
    os.environ["SERVER_PORT"] = str(free_port)
    os.environ["BALSAM_DATABASE_URL"] = setup_database

    gunicorn_config_file = Path(balsam.server.__file__).parent / "gunicorn.conf.example.py"
    assert gunicorn_config_file.is_file()
    proc = subprocess.Popen(
        [
            "gunicorn",
            "-c",
            gunicorn_config_file.as_posix(),
            "balsam.server.main:app",
        ]
    )

    url = f"http://localhost:{free_port}/"
    _server_health_check(url, timeout=10.0, check_interval=0.5)
    yield url
    proc.terminate()
    proc.communicate()
    return


def _server_health_check(url: str, timeout: float = 10.0, check_interval: float = 0.5) -> bool:
    """Make requests until getting a response"""
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


def _make_user_client(url: str) -> BasicAuthRequestsClient:
    """Create a basicauth client to the given url"""
    login_credentials: Dict[str, Any] = {"username": f"user{uuid4()}", "password": "test-password"}
    requests.post(
        url.rstrip("/") + "/" + urls.PASSWORD_REGISTER,
        json=login_credentials,
    )
    client = BasicAuthRequestsClient(url, **login_credentials)
    client.refresh_auth()
    return client


@pytest.fixture(scope="function")
def client_factory(live_server: str) -> Iterable[Callable[[], BasicAuthRequestsClient]]:
    """
    Returns factory for generating multiple clients per Test case.
    DELETES all Sites at the end of each test case.
    """
    created_clients: List[BasicAuthRequestsClient] = []

    def _create_client() -> BasicAuthRequestsClient:
        client = _make_user_client(live_server)
        created_clients.append(client)
        return client

    yield _create_client
    for client in created_clients:
        for site in client.Site.objects.all():
            site.delete()


@pytest.fixture(scope="function")
def client(client_factory: Callable[[], BasicAuthRequestsClient]) -> BasicAuthRequestsClient:
    """Single ephemeral client (sites will be cleaned up after test case)"""
    return client_factory()


@pytest.fixture(scope="module")
def temp_client_file() -> Iterable[str]:
    """Temporary file in ~/.balsam/_test for storing test credentials"""
    cred_dir = balsam_home().joinpath("_test")
    cred_dir.mkdir(parents=True, exist_ok=False)
    with tempfile.NamedTemporaryFile(mode="w", delete=False, dir=cred_dir, suffix=".yml") as fp:
        client_path = Path(fp.name).resolve().as_posix()
    yield client_path
    shutil.rmtree(cred_dir)


@pytest.fixture(scope="module")
def persistent_client(live_server: str, temp_client_file: str) -> Iterable[BasicAuthRequestsClient]:
    """
    Returns (client, client_settings_path) that persists for a full test module.
    The client can be used across all tests within a single module.
    Subprocesses and launchers must have BALSAM_CLIENT_PATH env to find the credentials.
    Cleans up all Sites at the end of each module.
    """
    client = _make_user_client(live_server)
    settings = ClientSettings(
        api_root=client.api_root,
        client_class="balsam.client.BasicAuthRequestsClient",
        token=client.token,
        token_expiry=client.token_expiry,
    )
    os.environ["BALSAM_CLIENT_PATH"] = temp_client_file
    settings.save_to_file()
    yield client

    for site in client.Site.objects.all():
        site.delete()
    del os.environ["BALSAM_CLIENT_PATH"]


@pytest.fixture(scope="module")
def balsam_site_config(persistent_client: BasicAuthRequestsClient, test_log_dir: Path) -> Iterable[SiteConfig]:
    """
    Create new Balsam Site/Apps for BALSAM_TEST_PLATFORM environ
    Yields the SiteConfig and cleans up Site at the end of module scope.
    """
    TEST_DEFAULTS_DIR = Path(__file__).parent / "default-configs"

    platform = get_platform()
    tmpdir_top = get_test_dir()
    configs_map = site_builder.load_default_configs(top=TEST_DEFAULTS_DIR)
    default_path = next(path for path in configs_map if path.name == platform)
    default_conf = configs_map[default_path]

    with tempfile.TemporaryDirectory(prefix="balsam-test-site", dir=tmpdir_top) as tmpdir:
        site_path = Path(tmpdir).joinpath("testsite")
        site_config = site_builder.new_site_setup(
            name="tmpsite",
            site_path=site_path,
            default_site_path=default_path,
            default_site_conf=default_conf,
            client=persistent_client,
            settings_template_path=TEST_DEFAULTS_DIR / "settings.yml.j2",
        )
        os.environ["BALSAM_SITE_PATH"] = str(site_path)
        sync_apps(site_config)
        yield site_config
        shutil.copytree(
            site_path.as_posix(),
            test_log_dir.joinpath("testsite").as_posix(),
        )

    persistent_client.Site.objects.get(id=site_config.site_id).delete()
    del os.environ["BALSAM_SITE_PATH"]


@pytest.fixture(scope="module")
def run_service(balsam_site_config: SiteConfig) -> Iterable[SiteConfig]:
    """
    Runs Balsam Site service at the Test Site for duration of module.
    """
    proc = start_site(balsam_site_config.site_path)
    yield balsam_site_config
    parent = psutil.Process(proc.pid)
    processes = parent.children(recursive=True)
    processes.append(parent)

    for child in processes:
        child.terminate()

    (gone, alive) = psutil.wait_procs(processes, timeout=15.0)
    for child in alive:
        try:
            print(f"WARNING! Force-killing process {child.pid}")
            child.kill()
        except (psutil.ProcessLookupError, psutil.NoSuchProcess):
            pass
