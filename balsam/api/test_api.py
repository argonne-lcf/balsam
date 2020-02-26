import pytest
from balsam.client import BasicAuthRequestsClient
from balsam.api.models import Site, SiteManager, App, AppBackend, AppManager


@pytest.yield_fixture(scope="session")
def django_db_setup(django_db_setup, django_db_blocker):
    """
    Fixture that will clean up remaining connections, that might be hanging
    from threads or external processes. Extending pytest_django.django_db_setup
    """
    yield
    with django_db_blocker.unblock():
        from django.db import connections

        conn = connections["default"]
        cursor = conn.cursor()
        cursor.execute("""SELECT * FROM pg_stat_activity;""")

        terminate_sql = (
            """
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = '%s'
                AND pid <> pg_backend_pid();
        """
            % conn.settings_dict["NAME"]
        )
        cursor.execute(terminate_sql)


@pytest.fixture(scope="function")
def client(live_server):
    url = live_server.url.rstrip("/") + "/api"
    from balsam.server.models import User

    try:
        User.objects.get(username="user")
    except User.DoesNotExist:
        User.objects.create_user(
            username="user",
            email="f@f.net",
            password="f",
            is_staff=True,
            is_superuser=True,
        )
    client = BasicAuthRequestsClient(api_root=url, username="user", password="f")
    SiteManager.register_client(client.sites)
    AppManager.register_client(client.apps)
    yield client


class TestSite:
    def create_several_hostnames(self):
        Site.objects.create(hostname="b3", path="/projects/foo")
        Site.objects.create(hostname="b1", path="/projects/foo")
        Site.objects.create(hostname="a2", path="/projects/foo")
        Site.objects.create(hostname="a1", path="/projects/foo")
        Site.objects.create(hostname="a3", path="/projects/foo")
        Site.objects.create(hostname="b2", path="/projects/foo")

    def test_create_and_list(self, client):
        assert len(Site.objects.all()) == 0
        Site.objects.create(hostname="theta", path="/projects/foo")
        Site.objects.create(hostname="cooley", path="/projects/bar")
        assert len(Site.objects.all()) == 2

    def test_create_via_save(self, client):
        newsite = Site(hostname="theta", path="/projects/foo")
        assert newsite.pk is None
        newsite.save()
        assert newsite.pk is not None

    def test_update_nested_status(self, client):
        site = Site.objects.create(hostname="theta", path="/projects/foo")
        pk = site.pk
        creation_ts = site.last_refresh

        site.status.num_nodes = 128
        site.status.num_idle_nodes = 127
        site.save()
        update_ts = site.last_refresh
        assert site.pk == pk
        assert update_ts > creation_ts

    def test_refresh_from_db(self, client):
        handle_1 = Site.objects.create(hostname="theta", path="/projects/foo")
        handle_2 = Site.objects.get(pk=handle_1.pk)
        assert handle_2.pk == handle_1.pk

        handle_2.status.num_nodes = 128
        handle_2.save()
        assert handle_2.last_refresh > handle_1.last_refresh

        handle_1.refresh_from_db()
        assert handle_2 == handle_1

    def test_delete(self, client):
        Site.objects.create(hostname="theta", path="/projects/foo")
        tempsite = Site.objects.create(hostname="cooley", path="/projects/bar")
        assert tempsite.pk is not None
        assert len(Site.objects.all()) == 2

        tempsite.delete()
        assert tempsite.pk is None
        sites = Site.objects.all()
        assert len(sites) == 1
        assert sites[0].hostname == "theta"

    def test_filter_on_hostname(self, client):
        Site.objects.create(hostname="theta", path="/projects/foo")
        Site.objects.create(hostname="theta", path="/projects/bar")
        Site.objects.create(hostname="cooley", path="/projects/baz")

        cooley_only = Site.objects.filter(hostname="cooley")
        assert len(cooley_only) == 1

        theta_only = Site.objects.filter(hostname="theta")
        assert len(theta_only) == 2

    def test_filter_on_pk_list(self, client):
        site1 = Site.objects.create(hostname="theta", path="/projects/foo")
        site2 = Site.objects.create(hostname="theta", path="/projects/bar")
        Site.objects.create(hostname="cooley", path="/projects/baz")
        pks = [site1.pk, site2.pk]
        qs = Site.objects.filter(pk=pks)
        assert len(qs) == 2

    def test_order_on_hostname(self, client):
        self.create_several_hostnames()
        qs = Site.objects.all().order_by("hostname")
        hosts = [site.hostname for site in qs]
        assert hosts == sorted(hosts)

    def test_limit(self, client):
        self.create_several_hostnames()
        qs = Site.objects.all().order_by("hostname")[:3]
        hosts = [site.hostname for site in qs]
        assert hosts == ["a1", "a2", "a3"]

    def test_limit_and_offset(self, client):
        self.create_several_hostnames()
        qs = Site.objects.all().order_by("hostname")[3:6]
        hosts = [site.hostname for site in qs]
        assert hosts == ["b1", "b2", "b3"]

    def test_get_by_pk_returns_match(self, client):
        site1 = Site.objects.create(hostname="theta", path="/projects/foo")
        retrieved = Site.objects.get(pk=site1.pk)
        assert site1 == retrieved

    def test_get_by_host_and_path_returns_match(self, client):
        site1 = Site.objects.create(hostname="theta", path="/projects/foo")
        site2 = Site.objects.create(hostname="theta", path="/projects/bar")
        retrieved = Site.objects.get(hostname="theta", path="/projects/bar")
        assert retrieved == site2
        assert retrieved.pk != site1.pk

    def test_get_raises_doesnotexist(self, client):
        with pytest.raises(Site.DoesNotExist):
            Site.objects.get(hostname="nonsense")

    def test_get_raises_multipleobj(self, client):
        Site.objects.create(hostname="theta", path="/projects/foo")
        Site.objects.create(hostname="theta", path="/projects/bar")
        with pytest.raises(Site.MultipleObjectsReturned):
            Site.objects.get(hostname="theta")

    def test_count_queryset(self, client):
        Site.objects.create(hostname="theta", path="/projects/foo")
        Site.objects.create(hostname="theta", path="/projects/bar")
        Site.objects.create(hostname="theta", path="/projects/baz")
        Site.objects.create(hostname="theta", path="/home/bar")
        assert Site.objects.filter(path__contains="projects").count() == 3
        assert Site.objects.filter(path__contains="home").count() == 1


class TestApps:
    def test_create_and_list(self, client):
        assert len(App.objects.all()) == 0

        site = Site.objects.create(hostname="theta", path="/projects/foo")
        backend = AppBackend(site=site, class_name="nwchem.GeomOpt")
        App.objects.create(
            name="nw-opt", backends=[backend], parameters=["geometry", "method"]
        )
        assert len(App.objects.all()) == 1

    def test_get_by_pk(self, client):
        site = Site.objects.create(hostname="theta", path="/projects/foo")
        backend = AppBackend(site=site, class_name="nwchem.GeomOpt")
        new_app = App.objects.create(
            name="nw-opt", backends=[backend], parameters=["geometry", "method"]
        )
        assert App.objects.get(pk=new_app.pk) == new_app

    def test_filter_by_owners_username(self, client):
        pass

    def test_filter_by_site_id(self, client):
        pass

    def test_filter_by_site_hostname(self, client):
        pass

    def test_update_parameters(self, client):
        pass

    def test_update_backends(self, client):
        pass

    def test_app_merge(self, client):
        pass
