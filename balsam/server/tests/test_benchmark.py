"""
Benchmarks for:
    - Job List
    - Job Bulk Create
    - Job Bulk Update (PATCH with statuses)
    - Session Acquire
    - Session Tick
"""
import pytest


@pytest.fixture(scope="function")
def populated_db_scenario(transactional_db):
    """
    - 100 users
    - 6 sites-per-user
    - 4 apps-per-site
    - 10,000 jobs per app
    - jobs updated to random states

    num_users = 10
    for i in range(num_users):
        User.objects.create_user(
            username=f"user{i}", email=f"user{i}@aol.com", password="a"
        )
    """
    pass
