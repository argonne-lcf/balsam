import os
from pathlib import Path
from typing import Optional, Set

PLATFORMS: Set[str] = {"alcf_theta", "alcf_thetagpu", "alcf_cooley", "alcf_polaris", "generic"}

LAUNCHER_STARTUP_TIMEOUT_SECONDS = {
    "generic": 30.0,
    "alcf_theta": 4 * 60.0 * 60,
    "alcf_thetagpu": 4 * 60.0 * 60,
    "alcf_cooley": 4 * 60.0 * 60,
    "alcf_polaris": 4 * 60.0 * 60,
}

LAUNCHER_SHUTDOWN_TIMEOUT_SECONDS = {
    "generic": 20.0,
    "alcf_theta": 120.0,
    "alcf_thetagpu": 120.0,
    "alcf_cooley": 120.0,
    "alcf_polaris": 120.0,
}


def get_platform() -> str:
    """
    Used to skip tests that are not applicable to the current platform
    """
    plat = os.environ.get("BALSAM_TEST_PLATFORM", "generic")
    return plat


def get_test_api_url() -> Optional[str]:
    """
    Test API server URL. If this is set, the Test DB is not used for
    API or integration tests (just run against a live server).
    """
    return os.environ.get("BALSAM_TEST_API_URL")


def get_test_db_url() -> str:
    """
    Test DB connection string. *Required* for standalone server tests.
    """
    return os.environ.get("BALSAM_TEST_DB_URL", "postgresql://postgres@localhost:5432/balsam-test")


def get_test_dir() -> Optional[str]:
    """
    Location for ephemeral Balsam test Sites.
    Must be mounted on launch, compute, and site-daemon nodes
    """
    return os.environ.get("BALSAM_TEST_DIR")


def get_test_log_dir() -> Path:
    """
    Log directory (persists as artifact after test session)
    Used by Live server (gunicorn).
    At the end of a Site integration test, the Site logs are copied
    into this directory as well.
    """
    base = os.environ.get("BALSAM_LOG_DIR") or Path.cwd()
    test_log_dir = Path(base).joinpath("pytest-logs")
    return test_log_dir


def get_launcher_startup_timeout() -> float:
    return LAUNCHER_STARTUP_TIMEOUT_SECONDS[get_platform()]


def get_launcher_shutdown_timeout() -> float:
    return LAUNCHER_SHUTDOWN_TIMEOUT_SECONDS[get_platform()]
