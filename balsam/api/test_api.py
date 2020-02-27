import pytest
from balsam.client import BasicAuthRequestsClient, DirectAPIClient
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
def requests_client(live_server):
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
    yield client


@pytest.fixture(scope="function")
def direct_client(transactional_db):
    url = "/api"
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
    client = DirectAPIClient(api_root=url, username="user", password="f")
    yield client


@pytest.fixture(scope="function", params=["requests_client", "direct_client"])
def client(requests_client, direct_client, request):
    """
    Run each test twice:
      1) Live server and real Requests Client
      2) Without server; using rest_framework.test.APIClient
    """
    test_client = (
        requests_client if request.param == "requests_client" else direct_client
    )
    SiteManager(test_client.sites)
    AppManager(test_client.apps)
    yield test_client


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

    def test_cannot_access_manager_from_instance(self, client):
        newsite = Site.objects.create(hostname="theta", path="/projects/foo")
        with pytest.raises(AttributeError):
            newsite.objects.count()

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
        site = Site.objects.create(hostname="theta", path="/projects/foo")
        backend = AppBackend(site=site, class_name="nwchem.GeomOpt")
        App.objects.create(
            name="nw-opt", backends=[backend], parameters=["geometry", "method"]
        )
        assert App.objects.filter(owner="user2").count() == 0
        assert App.objects.filter(owner="user1").count() == 0

    def test_filter_by_site_id(self, client):
        site1 = Site.objects.create(hostname="theta", path="/projects/foo")
        site2 = Site.objects.create(hostname="summit", path="/projects/bar")
        backend1 = AppBackend(site=site1, class_name="nwchem.GeomOpt")
        backend2 = AppBackend(site=site2, class_name="nwchem.GeomOpt")
        App.objects.create(
            name="dual_site", backends=[backend1, backend2], parameters=["geometry"]
        )
        App.objects.create(name="app1", backends=[backend1], parameters=["geometry"])
        App.objects.create(name="app2", backends=[backend2], parameters=["geometry"])
        app_names = set(a.name for a in App.objects.filter(site=site1.pk))
        assert app_names == {"dual_site", "app1"}
        app_names = set(a.name for a in App.objects.filter(site=site2.pk))
        assert app_names == {"dual_site", "app2"}

    def test_filter_by_site_hostname(self, client):
        site1 = Site.objects.create(hostname="theta", path="/projects/foo")
        site2 = Site.objects.create(hostname="summit", path="/projects/bar")
        backend1 = AppBackend(site=site1, class_name="nwchem.GeomOpt")
        backend2 = AppBackend(site=site2, class_name="nwchem.GeomOpt")
        App.objects.create(
            name="dual_site", backends=[backend1, backend2], parameters=["geometry"]
        )
        App.objects.create(name="app1", backends=[backend1], parameters=["geometry"])
        App.objects.create(name="app2", backends=[backend2], parameters=["geometry"])
        app_names = set(a.name for a in App.objects.filter(site_hostname="theta"))
        assert app_names == {"dual_site", "app1"}
        app_names = set(a.name for a in App.objects.filter(site_hostname="summit"))
        assert app_names == {"dual_site", "app2"}

    def test_update_parameters(self, client):
        site = Site.objects.create(hostname="theta", path="/projects/foo")
        backend = AppBackend(site=site, class_name="nwchem.GeomOpt")
        a = App.objects.create(
            name="nw-opt", backends=[backend], parameters=["geometry", "method"]
        )
        a.parameters = ["foo", "N", "w", "x"]
        a.save()

        assert App.objects.all().count() == 1
        retrieved = App.objects.get(pk=a.pk)
        assert retrieved.parameters == ["foo", "N", "w", "x"]

    def test_update_backends(self, client):
        site = Site.objects.create(hostname="theta", path="/projects/foo")
        backend1 = AppBackend(site=site, class_name="nwchem.GeomOpt")
        app = App.objects.create(
            name="nw-opt", backends=[backend1], parameters=["geometry", "method"]
        )

        backend1.class_name = "NWCHEM.gopt"
        site2 = Site.objects.create(hostname="cori", path="/projects/foo")
        backend2 = AppBackend(site=site2, class_name="NW.optimizer")
        app.backends = [backend1, backend2]
        app.save()

        retrieved = App.objects.get(pk=app.pk)
        assert retrieved.backends == [backend1, backend2]
        assert retrieved.backends[1].site_hostname == "cori"
        assert retrieved.backends[1].site_path.as_posix() == "/projects/foo"

    def test_app_merge(self, client):
        site1 = Site.objects.create(hostname="theta", path="/projects/foo")
        site2 = Site.objects.create(hostname="summit", path="/projects/bar")
        backend1 = AppBackend(site=site1, class_name="nwchem.GeomOpt")
        backend2 = AppBackend(site=site2, class_name="nwchem.GeomOpt")
        app1 = App.objects.create(
            name="app1", backends=[backend1], parameters=["geometry"]
        )
        app2 = App.objects.create(
            name="app2", backends=[backend2], parameters=["geometry"]
        )
        merged = App.objects.merge([app1, app2], name="dual_site")
        assert merged.backends == [backend1, backend2]
