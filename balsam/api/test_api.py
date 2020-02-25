import pytest
from balsam.client import BasicAuthRequestsClient
from .models import SiteManager, Site


@pytest.fixture(scope="session")
def django_db_setup(django_db_setup, django_db_blocker):
    with django_db_blocker.unblock():
        from balsam.server.models import User

        return User.objects.create_user(
            username="user",
            email="f@f.net",
            password="f",
            is_staff=True,
            is_superuser=True,
        )


@pytest.fixture(scope="function")
def client(live_server):
    url = live_server.url.rstrip("/") + "/api"
    client = BasicAuthRequestsClient(api_root=url, username="user", password="f")
    yield client


def test_site_list(client):
    SiteManager.register_client(client.sites)
    print(Site.objects.all())
