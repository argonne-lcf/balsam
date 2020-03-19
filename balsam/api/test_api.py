import os
import pytest
from urllib.parse import urlparse
from requests import HTTPError
from datetime import timedelta
from balsam.client import BasicAuthRequestsClient, DirectAPIClient
from .models import (  # noqa
    TransferState,
    TransferItem,
    JobState,
    Job,
    SiteStatus,
    Site,
    AppBackend,
    App,
    BatchState,
    BatchJob,
    NodeResources,
    Session,
    EventLog,
    utc_datetime,
)
from .managers import (
    JobManager,
    SiteManager,
    AppManager,
    BatchJobManager,
    SessionManager,
    EventLogManager,
)

try:
    test_db = os.environ["BALSAM_TEST_DB"]
except KeyError:
    raise KeyError("Environment BALSAM_TEST_DB must be set to host:port")
try:
    HOST, PORT = test_db.split(":")
    PORT = int(PORT)
except ValueError:
    raise ValueError(f"{test_db} is not a valid HOST:PORT")


def close_connections():
    from django.db import connections

    conn = connections["default"]
    cursor = conn.cursor()

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


@pytest.yield_fixture(scope="session")
def django_db_setup(django_db_setup, django_db_blocker):
    """
    Fixture that will clean up remaining connections, that might be hanging
    from threads or external processes. Extending pytest_django.django_db_setup
    """
    yield
    with django_db_blocker.unblock():
        close_connections()


@pytest.fixture(scope="function")
def requests_client(live_server, django_db_blocker):
    url = live_server.url.rstrip("/") + "/api"
    info = urlparse(url)
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
    host = info.netloc.split(":")[0]
    client = BasicAuthRequestsClient(
        scheme=info.scheme,
        host=host,
        port=info.port,
        api_root="/api",
        username="user",
        password="f",
    )
    yield client
    with django_db_blocker.unblock():
        close_connections()


@pytest.fixture(scope="function")
def direct_client(transactional_db, django_db_blocker):
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
    client = DirectAPIClient(
        api_root=url, host=HOST, port=PORT, username="user", password="f"
    )
    yield client
    with django_db_blocker.unblock():
        close_connections()


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
    JobManager(test_client.jobs)
    BatchJobManager(test_client.batch_jobs)
    SessionManager(test_client.sessions)
    EventLogManager(test_client.events)
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


class TestJobs:
    """Jobs and TransferItems"""

    def test_create(self, client):
        site = Site.objects.create(hostname="theta", path="/projects/foo")
        backend = AppBackend(site=site, class_name="nwchem.GeomOpt")
        app = App.objects.create(
            name="nw-opt", backends=[backend], parameters=["geometry"]
        )

        job = Job("test/say-hello", app, {"geometry": "test.xyz"}, ranks_per_node=64)
        assert job.pk is None
        job.save()
        assert job.pk is not None
        assert job.state == JobState.staged_in == "STAGED_IN"
        assert job.lock_status == "Unlocked"

    def test_bulk_create_and_update(self, client):
        site = Site.objects.create(hostname="theta", path="/projects/foo")
        backend = AppBackend(site=site, class_name="nwchem.GeomOpt")
        app = App.objects.create(
            name="nw-opt", backends=[backend], parameters=["geometry"]
        )
        jobs = [
            Job(f"test/{i}", app, {"geometry": "test.xyz"}, ranks_per_node=64)
            for i in range(3)
        ]
        assert all(job.state == "CREATED" for job in jobs)
        jobs = Job.objects.bulk_create(jobs)
        assert all(job.state == "STAGED_IN" for job in jobs)

        preproc_time = utc_datetime()
        for job in jobs:
            job.state = "PREPROCESSED"
            job.state_message = "Skipped Preprocessing Step"
            job.state_timestamp = preproc_time
        Job.objects.bulk_update(jobs, ["state", "state_message", "state_timestamp"])

        for job in Job.objects.all():
            assert job.state == "PREPROCESSED"
            assert job.state_message == ""
            assert job.state_timestamp is None

    def test_children_read(self, client):
        site = Site.objects.create(hostname="theta", path="/projects/foo")
        backend = AppBackend(site=site, class_name="nwchem.GeomOpt")
        app = App.objects.create(
            name="nw-opt", backends=[backend], parameters=["geometry"]
        )
        parent = Job(f"test/parent", app, {"geometry": "test.xyz"}, ranks_per_node=64)
        parent.save()
        child = Job(
            f"test/child",
            app,
            {"geometry": "test.xyz"},
            ranks_per_node=64,
            parents=[parent],
        )
        child.save()

        assert parent.state == "STAGED_IN"
        assert child.state == "AWAITING_PARENTS"
        assert child.parents == [parent.pk]
        assert parent.children == []
        parent.refresh_from_db()
        assert parent.children == [child.pk]

    def test_app_properties_read_only(self, client):
        site = Site.objects.create(hostname="theta", path="/projects/foo")
        backend = AppBackend(site=site, class_name="nwchem.GeomOpt")
        app = App.objects.create(
            name="nw-opt", backends=[backend], parameters=["geometry"]
        )
        job = Job("test/say-hello", app, {"geometry": "test.xyz"}, ranks_per_node=64)
        job.save()
        assert job.app == app.pk
        assert job.app_name == "nw-opt"
        assert job.site == "theta:/projects/foo"
        assert job.app_class == "nwchem.GeomOpt"

    def test_last_update_prop_changed_on_update(self, client):
        site = Site.objects.create(hostname="theta", path="/projects/foo")
        backend = AppBackend(site=site, class_name="nwchem.GeomOpt")
        app = App.objects.create(
            name="nw-opt", backends=[backend], parameters=["geometry"]
        )

        job = Job("test/say-hello", app, {"geometry": "test.xyz"}, ranks_per_node=64)
        job.save()
        t1 = job.last_update

        job.num_nodes *= 2
        job.save()
        job.refresh_from_db()
        assert job.num_nodes == 2
        assert job.last_update > t1

    def test_can_view_history(self, client):
        site = Site.objects.create(hostname="theta", path="/projects/foo")
        backend = AppBackend(site=site, class_name="nwchem.GeomOpt")
        app = App.objects.create(
            name="nw-opt", backends=[backend], parameters=["geometry"]
        )

        job = Job("test/say-hello", app, {"geometry": "test.xyz"}, ranks_per_node=64)
        job.save()
        states = [event.to_state for event in job.history()]
        assert states == ["STAGED_IN", "READY"]

        update_time = utc_datetime()
        job.state = "PREPROCESSED"
        job.state_message = "Skipped Preprocess: nothing to do"
        job.state_timestamp = update_time
        Job.objects.bulk_update([job], ["state", "state_message", "state_timestamp"])

        assert job.state == "PREPROCESSED"
        events = list(job.history())
        assert events[0].to_state == "PREPROCESSED"
        assert events[0].timestamp == update_time
        assert len(events) == 3

    def test_bulk_delete(self, client):
        site = Site.objects.create(hostname="theta", path="/projects/foo")
        backend = AppBackend(site=site, class_name="nwchem.GeomOpt")
        app = App.objects.create(
            name="nw-opt", backends=[backend], parameters=["geometry"]
        )

        jobs = [Job(f"test/{i}", app, {"geometry": "test.xyz"}) for i in range(3)]
        Job.objects.bulk_create(jobs)
        assert Job.objects.count() == 3
        Job.objects.all().delete(allow_delete_all=True)
        assert Job.objects.count() == 0

    def test_filter_by_tags(self, client):
        site = Site.objects.create(hostname="theta", path="/projects/foo")
        backend = AppBackend(site=site, class_name="nwchem.GeomOpt")
        app = App.objects.create(
            name="nw-opt", backends=[backend], parameters=["geometry"]
        )

        jobs = [
            Job(f"test/{i}", app, {"geometry": "test.xyz"}, tags={"foo": str(i)})
            for i in range(3)
        ]
        Job.objects.bulk_create(jobs)
        assert Job.objects.count() == 3
        qs = Job.objects.filter(tags__foo="1")
        assert qs.count() == 1
        assert qs[0].workdir.as_posix() == "test/1"

    def test_filter_by_pk_list(self, client):
        site = Site.objects.create(hostname="theta", path="/projects/foo")
        backend = AppBackend(site=site, class_name="nwchem.GeomOpt")
        app = App.objects.create(
            name="nw-opt", backends=[backend], parameters=["geometry"]
        )

        foo_jobs = [Job(f"foo/{i}", app, {"geometry": "foo"}) for i in range(3)]
        foo_jobs = Job.objects.bulk_create(foo_jobs)
        pks = [j.pk for j in foo_jobs]

        bar_jobs = [Job(f"bar/{i}", app, {"geometry": "bar"}) for i in range(3)]
        bar_jobs = Job.objects.bulk_create(bar_jobs)

        assert Job.objects.count() == 6
        assert Job.objects.filter(pk=pks).count() == 3

    def test_state_ordering(self, client):
        site = Site.objects.create(hostname="theta", path="/projects/foo")
        backend = AppBackend(site=site, class_name="nwchem.GeomOpt")
        app = App.objects.create(
            name="nw-opt", backends=[backend], parameters=["geometry"]
        )

        jobs = [Job(f"foo/{i}", app, {"geometry": "foo"}) for i in range(4)]
        jobs = Job.objects.bulk_create(jobs)
        jobs[2].state = "PREPROCESSED"
        jobs[2].save()

        states = [job.state for job in Job.objects.all().order_by("state")]
        assert states == ["PREPROCESSED"] + ["STAGED_IN"] * 3

    def test_filter_by_workdir(self, client):
        site = Site.objects.create(hostname="theta", path="/projects/foo")
        backend = AppBackend(site=site, class_name="nwchem.GeomOpt")
        app = App.objects.create(
            name="nw-opt", backends=[backend], parameters=["geometry"]
        )

        jobs = [Job(f"foo/{i}", app, {"geometry": "foo"}) for i in range(4)]
        jobs.append(Job(f"bar/99", app, {"geometry": "foo"}))
        jobs = Job.objects.bulk_create(jobs)

        assert Job.objects.filter(workdir="foo/2").count() == 1
        assert Job.objects.filter(workdir="foo/8").count() == 0
        assert Job.objects.filter(workdir__contains="foo").count() == 4
        assert Job.objects.filter(workdir__contains="bar").count() == 1

    def test_filter_by_site(self, client):
        site1 = Site.objects.create(hostname="theta", path="/projects/foo")
        backend1 = AppBackend(site=site1, class_name="nwchem.GeomOpt")
        site2 = Site.objects.create(hostname="summit", path="/projects/foo")
        backend2 = AppBackend(site=site2, class_name="nwchem.GeomOpt")

        # Create a dual-backend app
        app = App.objects.create(
            name="nw-opt", backends=[backend1, backend2], parameters=["geometry"]
        )

        jobs = [Job(f"foo/{i}", app, {"geometry": "foo"}) for i in range(4)]
        jobs = Job.objects.bulk_create(jobs)

        # Site1 and Site 2 each acquire 2 jobs
        sess1 = Session.objects.create(site=site1)
        acquired1 = sess1.acquire_jobs(
            acquire_unbound=True, states=["READY"], max_num_acquire=2
        )
        assert len(acquired1) == 2
        sess2 = Session.objects.create(site=site2)
        acquired2 = sess2.acquire_jobs(
            acquire_unbound=True, states=["READY"], max_num_acquire=2
        )
        assert len(acquired2) == 2

        # Check site filters
        Job.objects.filter(app_class="nwchem.GeomOpt").count() == 4
        Job.objects.filter(site_hostname="theta").count() == 2
        Job.objects.filter(site_hostname="summit").count() == 2

    def test_filter_by_parameters(self, client):
        site = Site.objects.create(hostname="theta", path="/projects/foo")
        backend = AppBackend(site=site, class_name="nwchem.GeomOpt")
        app = App.objects.create(
            name="nw-opt", backends=[backend], parameters=["geometry"]
        )

        jobs = [Job(f"foo/{i}", app, {"geometry": f"{i}"}) for i in range(4)]
        jobs = Job.objects.bulk_create(jobs)

        assert Job.objects.filter(parameters__geometry="4").count() == 0
        assert Job.objects.filter(parameters__geometry="3").count() == 1

    def test_filter_by_state(self, client):
        site = Site.objects.create(hostname="theta", path="/projects/foo")
        backend = AppBackend(site=site, class_name="nwchem.GeomOpt")
        app = App.objects.create(
            name="nw-opt", backends=[backend], parameters=["geometry"]
        )

        jobs = [Job(f"foo/{i}", app, {"geometry": "foo"}) for i in range(4)]
        jobs = Job.objects.bulk_create(jobs)
        jobs[2].state = "PREPROCESSED"
        jobs[2].save()

        assert Job.objects.filter(state="STAGED_IN").count() == 3
        assert Job.objects.filter(state="PREPROCESSED").count() == 1
        assert Job.objects.filter(state__ne="STAGED_IN").count() == 1
        assert Job.objects.filter(state__ne="PREPROCESSED").count() == 3


class TestEvents:
    def setup_scenario(self):
        site = Site.objects.create(hostname="theta", path="/projects/foo")
        backend = AppBackend(site=site, class_name="nwchem.GeomOpt")
        app = App.objects.create(
            name="nw-opt", backends=[backend], parameters=["geometry"]
        )

        before_creation = utc_datetime()

        j1 = Job.objects.create("foo/1", app, {"geometry": "foo"})
        j2 = Job.objects.create("foo/2", app, {"geometry": "foo"})
        j3 = Job.objects.create("foo/3", app, {"geometry": "foo"})

        j1.state = "PREPROCESSED"
        j1.save()

        j2.state = "PREPROCESSED"
        j2.save()
        j2.state = "RUNNING"
        j2.save()
        j2.state = "RUN_ERROR"
        j2.state_timestamp = utc_datetime() + timedelta(minutes=1)
        j2.save()

        j3.state = "PREPROCESSED"
        j3.save()
        j3.state = "RUNNING"
        j3.save()
        j3.state = "RUN_DONE"
        j3.state_message = "OK: done!"
        j3.save()
        return before_creation

    def test_filter_by_job(self, client):
        self.setup_scenario()
        pk = Job.objects.get(workdir="foo/2").pk
        qs = EventLog.objects.filter(job_id=pk)
        states = [event.to_state for event in qs]
        assert states == ["RUN_ERROR", "RUNNING", "PREPROCESSED", "STAGED_IN", "READY"]

    def test_filter_by_to_state(self, client):
        self.setup_scenario()
        assert EventLog.objects.filter(to_state="RUN_ERROR").count() == 1
        assert EventLog.objects.filter(to_state="READY").count() == 3

    def test_filter_by_from_state(self, client):
        self.setup_scenario()
        assert EventLog.objects.filter(from_state="CREATED").count() == 3

    def test_filter_by_to_and_from_state(self, client):
        self.setup_scenario()
        qs = EventLog.objects.filter(from_state="RUNNING").filter(to_state="RUN_ERROR")
        assert qs.count() == 1
        assert qs[0] == EventLog.objects.get(from_state="RUNNING", to_state="RUN_ERROR")

    def test_filter_by_message(self, client):
        self.setup_scenario()
        qs = EventLog.objects.filter(message__contains="done!")
        assert qs.count() == 1
        assert qs[0].to_state == "RUN_DONE"

    def test_filter_by_timestamp_range(self, client):
        self.setup_scenario()
        t = utc_datetime() + timedelta(seconds=30)
        assert EventLog.objects.filter(timestamp_after=t).count() == 1
        assert EventLog.objects.filter(timestamp_before=t).count() == 12

    def test_cannot_create_or_update(self, client):
        with pytest.raises(HTTPError) as e:
            EventLog.objects.create(
                job_id=1,
                from_state="RUNNING",
                to_state="RUN_DONE",
                timestamp=utc_datetime(),
                message="",
            )
        assert '"POST" not allowed' in str(e)


class TestBatchJobs:
    def test_create(self, client):
        site = Site.objects.create(hostname="theta", path="/projects/foo")
        bjob = BatchJob.objects.create(
            site=site,
            project="datascience",
            queue="default",
            num_nodes=128,
            wall_time_min=30,
            job_mode="mpi",
            filter_tags={"system": "H2O", "calc_type": "energy"},
        )
        assert bjob.state == BatchState.pending_submission
        assert bjob.pk is not None

    def test_bulk_update(self, client):
        site = Site.objects.create(hostname="theta", path="/projects/foo")
        for i in range(3):
            BatchJob.objects.create(
                site=site,
                project="datascience",
                queue="default",
                num_nodes=128,
                wall_time_min=30,
                job_mode="mpi",
                filter_tags={"system": "H2O", "calc_type": "energy"},
            )

        assert BatchJob.objects.count() == 3

        bjobs = BatchJob.objects.filter(site=site.pk)
        for job, sched_id in zip(bjobs, [123, 124, 125]):
            job.state = BatchState.queued
            job.scheduler_id = sched_id
        BatchJob.objects.bulk_update(bjobs, ["state", "scheduler_id"])

        after_update = list(BatchJob.objects.filter(site=site.pk))
        assert after_update == list(bjobs)

    def test_bulk_update_with_revert(self, client):
        site = Site.objects.create(hostname="theta", path="/projects/foo")
        for i in range(3):
            BatchJob.objects.create(
                site=site,
                project="datascience",
                queue="default",
                num_nodes=128,
                wall_time_min=30,
                job_mode="mpi",
                filter_tags={"system": "H2O", "calc_type": "energy"},
            )

        # Jobs are queued
        updated = BatchJob.objects.all().update(state=BatchState.queued)
        assert all(job.state == BatchState.queued for job in updated)

        # Client updates wall_time
        BatchJob.objects.all().update(wall_time_min=45)

        # The job had already started running
        jobs = BatchJob.objects.filter(site=site.pk)
        for job in jobs:
            job.state = BatchState.running

        BatchJob.objects.bulk_update(jobs, ["state"])

        # Revert wall time
        jobs = BatchJob.objects.filter(site=site.pk)
        for job in jobs:
            assert job.wall_time_min == 45
            job.wall_time_min = 30

        # Without revert, update fails:
        with pytest.raises(HTTPError):
            BatchJob.objects.bulk_update(jobs, ["wall_time_min"])

        for job in jobs:
            assert job.wall_time_min == 30
            job.revert = True
        BatchJob.objects.bulk_update(jobs, ["wall_time_min", "revert"])

        # Update worked:
        assert all(job.wall_time_min == 30 for job in BatchJob.objects.all())

    def test_delete(self, client):
        site = Site.objects.create(hostname="theta", path="/projects/foo")
        for i in range(3):
            BatchJob.objects.create(
                site=site,
                project="datascience",
                queue="default",
                num_nodes=128,
                wall_time_min=30,
                job_mode="mpi",
                filter_tags={"system": "H2O", "calc_type": "energy"},
            )
        assert BatchJob.objects.count() == 3

        with pytest.raises(NotImplementedError):
            BatchJob.objects.filter(site=site.pk).delete()

        for job in BatchJob.objects.all():
            job.delete()
            assert job.pk is None
        assert BatchJob.objects.count() == 0

    def test_filter_by_tags(self, client):
        site = Site.objects.create(hostname="theta", path="/projects/foo")
        for system in ["H2O", "D2O", "NH3", "CH2O"]:
            BatchJob.objects.create(
                site=site,
                project="datascience",
                queue="default",
                num_nodes=128,
                wall_time_min=30,
                job_mode="mpi",
                filter_tags={"system": system, "calc_type": "energy"},
            )

        assert BatchJob.objects.filter(filter_tags__system="NH3").count() == 1

    def test_get_by_scheduler_id(self, client):
        site = Site.objects.create(hostname="theta", path="/projects/foo")
        job = BatchJob.objects.create(
            site=site,
            project="datascience",
            queue="default",
            num_nodes=128,
            wall_time_min=30,
            job_mode="mpi",
            filter_tags={"system": "H2O", "calc_type": "energy"},
        )
        job.scheduler_id = 1234
        job.state = "queued"
        job.save()
        job = BatchJob.objects.get(scheduler_id=1234)
        assert job.num_nodes == 128

    def test_fetch_associated_jobs(self, client):
        site = Site.objects.create(hostname="theta", path="/projects/foo")
        backend = AppBackend(site=site, class_name="nwchem.GeomOpt")
        app = App.objects.create(
            name="nw-opt", backends=[backend], parameters=["geometry"]
        )

        batch_job = BatchJob.objects.create(
            site=site,
            project="datascience",
            queue="default",
            num_nodes=128,
            wall_time_min=30,
            job_mode="mpi",
            filter_tags={"system": "H2O", "calc_type": "energy"},
        )
        batch_job.scheduler_id = 1234
        batch_job.state = "running"
        batch_job.save()

        for i in range(3):
            job = Job.objects.create(f"test/{i}", app, {"geometry": "a.xyz"})
            assert job.batch_job is None

        sess = Session.objects.create(site=site, batch_job=batch_job)
        acquired = sess.acquire_jobs(
            acquire_unbound=False, states=["STAGED_IN"], max_num_acquire=10
        )
        assert len(acquired) == 3

        related = sorted(list(batch_job.jobs()), key=lambda job: job.pk)
        acquired = sorted(list(acquired), key=lambda job: job.pk)
        assert len(related) == 3
        assert related == acquired


class TestSessions:
    def create_site_app(self):
        site = Site.objects.create(hostname="theta", path="/projects/foo")
        backend = AppBackend(site=site, class_name="nwchem.GeomOpt")
        app = App.objects.create(
            name="nw-opt", backends=[backend], parameters=["geometry"]
        )
        return site, app

    def job(self, name, app, args={"geometry": "test.xyz"}, **kwargs):
        return Job(f"test/{name}", app, args, **kwargs)

    def create_jobs(self, app, num_jobs=3):
        jobs = [self.job(i, app) for i in range(num_jobs)]
        return Job.objects.bulk_create(jobs)

    def test_create(self, client):
        before_create = utc_datetime()
        site = Site.objects.create(hostname="theta", path="/projects/foo")
        sess = Session.objects.create(site=site)
        assert sess.heartbeat > before_create

    def test_acquire(self, client):
        site, app = self.create_site_app()
        jobs = self.create_jobs(app, num_jobs=3)
        for job in jobs:
            assert job.lock_status == "Unlocked"

        sess = Session.objects.create(site=site)
        acquired = sess.acquire_jobs(
            acquire_unbound=False, states=["STAGED_IN"], max_num_acquire=10
        )
        assert len(acquired) == 3
        for job in acquired:
            assert job.lock_status == "Preprocessing"

    def test_acquire_with_batch_job(self, client):
        site, app = self.create_site_app()
        jobs = self.create_jobs(app, num_jobs=3)
        for job in jobs:
            job.state = "PREPROCESSED"
        Job.objects.bulk_update(jobs, ["state"])
        for job in jobs:
            assert job.batch_job is None

        bjob = BatchJob.objects.create(
            site=site,
            project="datascience",
            queue="default",
            num_nodes=128,
            wall_time_min=30,
            job_mode="mpi",
            filter_tags={"system": "H2O", "calc_type": "energy"},
        )

        sess = Session.objects.create(site=site, batch_job=bjob)
        acquired = sess.acquire_jobs(
            acquire_unbound=False, states=["PREPROCESSED"], max_num_acquire=10
        )
        assert len(acquired) == 3
        for job in acquired:
            assert job.lock_status == "Acquired by launcher"
            assert job.batch_job == bjob.pk

        for job in jobs:
            job.refresh_from_db()

        assert list(sorted(jobs, key=lambda x: x.pk)) == list(
            sorted(acquired, key=lambda x: x.pk)
        )

    def test_acquire_with_node_resources(self, client):
        site, app = self.create_site_app()
        job_reqs = [
            dict(wall_time_min=31, threads_per_rank=4, node_packing_count=4),
            dict(wall_time_min=40, threads_per_rank=1, node_packing_count=4),
            dict(wall_time_min=32, threads_per_rank=4, node_packing_count=4),
            dict(wall_time_min=33, threads_per_rank=4, node_packing_count=4),
        ]
        jobs = [self.job(i, app, **req) for i, req in enumerate(job_reqs)]
        jobs = Job.objects.bulk_create(jobs)
        Job.objects.all().update(state="PREPROCESSED", state_message="foo")

        sess = Session.objects.create(site=site)
        available_resources = NodeResources(
            max_jobs_per_node=8,
            max_wall_time_min=35,
            running_job_counts=[2, 0],
            node_occupancies=[0.6, 0.0],
            idle_cores=[3, 8],
            idle_gpus=[0, 0],
        )
        acquired = sess.acquire_jobs(
            acquire_unbound=False,
            states=["PREPROCESSED", "RESTART_READY"],
            max_num_acquire=16,
            node_resources=available_resources,
            order_by=("-wall_time_min",),
        )
        assert len(acquired) == 2
        wall_times = [j.wall_time_min for j in acquired]
        assert wall_times == [33, 32]

    def test_acquire_with_filter_tags(self, client):
        site, app = self.create_site_app()

        job_tags = [
            dict(system="H2O", type="energy"),
            dict(system="D2O", type="energy"),
            dict(system="NO2", type="energy"),
            dict(system="H2O", type="gradient"),
        ]
        jobs = [self.job(i, app, tags=tags) for i, tags in enumerate(job_tags)]
        jobs = Job.objects.bulk_create(jobs)

        sess = Session.objects.create(site=site)
        acquired = sess.acquire_jobs(
            acquire_unbound=False,
            states=["STAGED_IN"],
            max_num_acquire=10,
            filter_tags={"system": "H2O", "type": "energy"},
        )
        assert len(acquired) == 1
        assert acquired[0].tags == dict(system="H2O", type="energy")

    def test_tick(self, client):
        site = Site.objects.create(hostname="theta", path="/projects/foo")
        sess = Session.objects.create(site=site)
        creation_time = sess.heartbeat
        sess.save()
        assert sess.heartbeat > creation_time

    def test_delete(self, client):
        site, app = self.create_site_app()
        jobs = self.create_jobs(app, num_jobs=3)
        for job in jobs:
            assert job.lock_status == "Unlocked"

        sess = Session.objects.create(site=site)
        acquired = sess.acquire_jobs(
            acquire_unbound=False, states=["STAGED_IN"], max_num_acquire=10
        )
        assert len(acquired) == 3
        for job in acquired:
            assert job.lock_status == "Preprocessing"

        assert sess.pk is not None
        sess.delete()
        assert sess.pk is None

        for job in acquired:
            job.refresh_from_db()
            assert job.lock_status == "Unlocked"
