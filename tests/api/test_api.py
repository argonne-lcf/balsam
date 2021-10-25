import random
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from balsam._api.app import ApplicationDefinition
from balsam.schemas import TransferItemState

GeomOpt = None
AppA = None
AppB = None
AppC = None
AppPy = None


@pytest.fixture(scope="function")
def appdef(client):
    global GeomOpt

    class GeomOpt(ApplicationDefinition):
        site = 0
        command_template = "/path/nwchem-runner {{ method }} {{ geometry }}"
        parameters = {
            "geometry": {"required": True},
            "method": {"required": False, "default": "HF"},
        }

    GeomOpt._set_client(client)
    return GeomOpt


@pytest.fixture(scope="function")
def appdef_a(client):
    global AppA

    class AppA(ApplicationDefinition):
        site = 0
        command_template = "echo hello"

    AppA._set_client(client)
    return AppA


@pytest.fixture(scope="function")
def appdef_b(client):
    global AppB

    class AppB(ApplicationDefinition):
        site = 0
        command_template = "echo hello"

    AppB._set_client(client)
    return AppB


@pytest.fixture(scope="function")
def appdef_c(client):
    global AppC

    class AppC(ApplicationDefinition):
        site = 0
        command_template = "echo hello"

    AppC._set_client(client)
    return AppC


@pytest.fixture(scope="function")
def appdef_py(client):
    global AppPy

    class AppPy(ApplicationDefinition):
        """Adds two numbers"""

        site = 0

        def run(self, x: int, y: int) -> int:
            return x + y

    AppPy._set_client(client)
    return AppPy


class TestSite:
    def test_create_and_list(self, client):
        Site = client.Site
        assert len(Site.objects.all()) == 0
        s1 = Site.objects.create(name="theta", path="/projects/foo")
        s2 = Site.objects.create(name="cooley", path="/projects/bar")
        assert len(Site.objects.all()) == 2
        assert s1.id is not None
        assert "foo" in s1.path.as_posix()
        assert "bar" in s2.path.as_posix()

    def test_create_via_save(self, client):
        Site = client.Site
        newsite = Site(name="theta", path="/projects/foo")
        assert newsite.id is None
        assert newsite._state == "creating"
        newsite.save()
        assert newsite._state == "clean"
        assert newsite.id is not None

    def test_update_status(self, client):
        Site = client.Site
        site = Site.objects.create(name="theta", path="/projects/foo")
        id = site.id
        creation_ts = site.last_refresh

        site.backfill_windows = {"default": [{"num_nodes": 31, "wall_time_min": 45}]}

        site.save()
        update_ts = site.last_refresh
        assert site.id == id
        assert update_ts > creation_ts
        assert len(site.backfill_windows) == 1

    def test_refresh_from_db(self, client):
        Site = client.Site
        handle_1 = Site.objects.create(name="theta", path="/projects/foo")
        handle_2 = Site.objects.get(id=handle_1.id)
        assert handle_2.id == handle_1.id

        handle_2.backfill_windows = {"default": [{"num_nodes": 25, "wall_time_min": 35}]}
        handle_2.save()
        assert handle_2.last_refresh > handle_1.last_refresh

        handle_1.refresh_from_db()
        assert handle_2 == handle_1

    def test_delete(self, client):
        Site = client.Site
        Site.objects.create(name="theta", path="/projects/foo")
        tempsite = Site.objects.create(name="cooley", path="/projects/bar")
        assert tempsite.id is not None
        assert len(Site.objects.all()) == 2

        tempsite.delete()
        assert tempsite.id is None
        sites = Site.objects.all()
        assert len(sites) == 1
        assert sites[0].name == "theta"

    def test_filter_on_name(self, client):
        Site = client.Site
        Site.objects.create(name="thetalogin3.alcf.anl.gov", path="/projects/foo")
        Site.objects.create(name="thetalogin4.alcf.anl.gov", path="/projects/bar")
        Site.objects.create(name="cooley", path="/projects/baz")

        cooley_only = Site.objects.filter(name="cooley")
        assert len(cooley_only) == 1

        theta_substr = Site.objects.filter(name="theta")
        assert len(theta_substr) == 0

    def test_get_by_id_returns_match(self, client):
        Site = client.Site
        site1 = Site.objects.create(name="theta", path="/projects/foo")
        retrieved = Site.objects.get(id=site1.id)
        assert site1 == retrieved

    def test_get_by_name_or_path_returns_match(self, client):
        Site = client.Site
        site1 = Site.objects.create(name="theta1", path="/projects/foo")
        site2 = Site.objects.create(name="theta2", path="/projects/bar")
        retrieved = Site.objects.get(name="theta2")
        assert retrieved == site2
        assert retrieved.id == site2.id
        retrieved = Site.objects.get(path="foo")
        assert retrieved == site1
        assert retrieved.id == site1.id

    def test_get_raises_doesnotexist(self, client):
        Site = client.Site
        with pytest.raises(Site.DoesNotExist):
            Site.objects.get(name="nonsense")

    def test_get_raises_multipleobj(self, client):
        Site = client.Site
        Site.objects.create(name="theta1", path="/projects/foo")
        Site.objects.create(name="theta2", path="/projects/foo2")
        with pytest.raises(Site.MultipleObjectsReturned):
            Site.objects.get(path="foo")

    def test_count_queryset(self, client):
        Site = client.Site
        Site.objects.create(name="theta1", path="/projects/foo")
        Site.objects.create(name="theta2", path="/projects/bar")
        Site.objects.create(name="theta3", path="/projects/baz")
        Site.objects.create(name="theta4", path="/home/bar")
        assert Site.objects.filter(path="/projects/").count() == 3
        assert Site.objects.filter(path="/home/").count() == 1


class TestApps:
    def test_create_and_list(self, client, appdef):
        App = client.App
        Site = client.Site
        assert len(App.objects.all()) == 0

        mysite = Site.objects.create(name="theta", path="/projects/foo")
        GeomOpt = appdef
        GeomOpt.site = mysite
        assert GeomOpt.__app_id__ is None
        GeomOpt.sync()
        assert len(App.objects.all()) == 1
        assert GeomOpt.__app_id__ is not None

    def test_get_by_id(self, client, appdef):
        App = client.App
        Site = client.Site
        site = Site.objects.create(name="theta", path="/projects/foo")
        GeomOpt = appdef
        GeomOpt.site = site
        GeomOpt.sync()
        assert App.objects.get(id=GeomOpt.__app_id__).name == "GeomOpt"

    def test_filter_by_site_id(self, client, appdef_a, appdef_b, appdef_c):
        App = client.App
        Site = client.Site
        site1 = Site.objects.create(name="theta", path="/projects/foo")
        site2 = Site.objects.create(name="summit", path="/projects/bar")

        A = appdef_a
        B = appdef_b
        C = appdef_c

        A.site = site1.id
        A.sync()
        B.site = site1.id
        B.sync()
        C.site = site2.id
        C.sync()

        assert App.objects.filter(site_id=site1.id).count() == 2
        assert App.objects.filter(site_id=site2.id).count() == 1

    def test_update_parameters(self, client):
        App = client.App
        Site = client.Site
        site = Site.objects.create(name="theta", path="/projects/foo")
        app = App.objects.create(
            site_id=site.id, name="OneApp", parameters={}, serialized_class="txt", source_code="txt"
        )
        assert app.parameters == {}
        app.parameters = {"foo": {"required": "True"}}
        app.save()

        assert App.objects.all().count() == 1
        retrieved = App.objects.get(id=app.id)
        assert "foo" in retrieved.parameters.keys()

    def test_deserializes(self, client, appdef_py):
        Site = client.Site
        site = Site.objects.create(name="theta", path="/projects/foo")
        AppPy = appdef_py
        AppPy.site = site
        AppPy.sync()

        loaded_app = client.App.objects.get(name="AppPy")
        assert "Adds two numbers" in loaded_app.description
        assert "return x + y" in loaded_app.source_code

        loaded_appdef = ApplicationDefinition.from_serialized(loaded_app)

        assert loaded_appdef.__app_id__ == AppPy.__app_id__
        assert loaded_appdef.run(None, 5, 4) == 9


class TestJobs:
    """Jobs and TransferItems"""

    def test_create(self, client):
        App = client.App
        Site = client.Site
        Job = client.Job
        site = Site.objects.create(name="theta", path="/projects/foo")
        app = App.objects.create(
            site_id=site.id,
            name="one",
            parameters={"geometry": {"required": True}},
            serialized_class="txt",
            source_code="txt",
        )

        job = Job("test/run1", app_id=app.id, parameters={"geometry": "test.xyz"}, ranks_per_node=64)
        assert job.id is None
        job.save()
        assert job.id is not None
        assert job.state == "STAGED_IN"

    def test_create_using_app_name(self, client, appdef):
        Site = client.Site
        Job = client.Job
        site = Site.objects.create(name="theta", path="/projects/foo")
        GeomOpt = appdef
        GeomOpt.site = site
        GeomOpt.sync()

        job = Job("test/run1", app_id="GeomOpt", parameters={"geometry": "test.xyz"}, ranks_per_node=64)
        assert job.id is None
        job.save()
        assert job.id is not None
        assert job.app_id == GeomOpt.__app_id__
        assert job.state == "STAGED_IN"

    def test_create_using_app_submit_method(self, client, appdef):
        Site = client.Site
        site = Site.objects.create(name="theta", path="/projects/foo")
        GeomOpt = appdef
        GeomOpt.site = site
        GeomOpt.sync()

        job = GeomOpt.submit(workdir="test/35", geometry="h2o.xyz")
        assert job.id is not None
        assert job.get_parameters() == {"geometry": "h2o.xyz"}
        assert job.app_id == GeomOpt.__app_id__
        assert job.state == "STAGED_IN"

    def test_set_and_fetch_data(self, client):
        App = client.App
        Site = client.Site
        Job = client.Job
        site = Site.objects.create(name="theta", path="/projects/foo")
        app = App.objects.create(site_id=site.id, name="one", serialized_class="txt", source_code="txt")

        job = Job.objects.create("test/run1", app_id=app.id)
        assert job.id is not None

        job.data = {"foo": 1234}
        job.save()
        retrieved = Job.objects.get(id=job.id)
        assert retrieved.data == {"foo": 1234}

    def test_update_data(self, client):
        App = client.App
        Site = client.Site
        Job = client.Job
        site = Site.objects.create(name="theta", path="/projects/foo")
        app = App.objects.create(site_id=site.id, name="one", serialized_class="txt", source_code="txt")
        job = Job.objects.create("test/run1", app_id=app.id, data={"foo": 1234}, num_nodes=1)

        # Correct way to update a mutable field:
        data = job.data
        data["bar"] = 456
        job.data = data
        job.save()
        job.refresh_from_db()
        assert "bar" in job.data

        # Wrong way to update a mutable field:
        job.data["baz"] = 789
        job.save()
        job.refresh_from_db()
        assert "baz" not in job.data

    def test_order_limit_offset(self, client):
        App = client.App
        Site = client.Site
        Job = client.Job
        site = Site.objects.create(name="theta", path="/projects/foo")
        app = App.objects.create(site_id=site.id, name="one", serialized_class="txt", source_code="txt")
        jobs = [Job(f"test/{i}", app_id=app.id) for i in range(8)]
        random.shuffle(jobs)
        Job.objects.bulk_create(jobs)

        subset = Job.objects.all().order_by("workdir")[:4]
        assert len(subset) == 4
        assert set(job.workdir.as_posix() for job in subset) == {f"test/{i}" for i in range(4)}
        subset = Job.objects.all().order_by("workdir")[5:]
        assert len(subset) == 3
        assert set(job.workdir.as_posix() for job in subset) == {f"test/{i}" for i in range(5, 8)}
        subset = Job.objects.all().order_by("-workdir")[5:7]
        assert len(subset) == 2
        assert set(job.workdir.as_posix() for job in subset) == {"test/2", "test/1"}
        subset = Job.objects.all().order_by("workdir")[5:7]
        assert len(subset) == 2
        assert set(job.workdir.as_posix() for job in subset) == {"test/5", "test/6"}

    def test_bulk_create_and_update(self, client):
        App = client.App
        Site = client.Site
        Job = client.Job
        site = Site.objects.create(name="theta", path="/projects/foo")
        app = App.objects.create(site_id=site.id, name="one", serialized_class="txt", source_code="txt")
        jobs = [Job(f"test/{i}", app_id=app.id) for i in range(10)]

        jobs = Job.objects.bulk_create(jobs)
        assert all(job.state == "STAGED_IN" for job in jobs)

        preproc_time = datetime.now(timezone.utc)
        for job in jobs:
            job.state = "PREPROCESSED"
            job.state_data = {"message": "Skipped Preprocessing Step"}
            job.state_timestamp = preproc_time

        Job.objects.bulk_update(jobs)

        # Jobs appear updated in fresh query:
        for job in Job.objects.all():
            assert job._state == "clean"
            assert job.state == "PREPROCESSED"
            assert job.last_update > preproc_time
            assert job.state_data is None
            assert job.state_timestamp is None

    def test_children_read(self, client):
        App = client.App
        Site = client.Site
        Job = client.Job
        site = Site.objects.create(name="theta", path="/projects/foo")
        app = App.objects.create(site_id=site.id, name="one", serialized_class="txt", source_code="txt")
        parent = Job("test/parent", app_id=app.id)
        parent.save()
        child = Job("test/child", app_id=app.id, parent_ids=[parent.id])
        child.save()

        assert parent.state == "STAGED_IN"
        assert child.state == "AWAITING_PARENTS"
        assert child.parent_ids == {parent.id}
        assert child.parent_query().count() == 1

    def test_last_update_prop_changed_on_update(self, client):
        App = client.App
        Site = client.Site
        Job = client.Job
        site = Site.objects.create(name="theta", path="/projects/foo")
        app = App.objects.create(
            site_id=site.id,
            name="one",
            parameters={"geometry": {"required": False, "default": "inp.xyz"}},
            serialized_class="txt",
            source_code="txt",
        )
        job = Job("test/test", app_id=app.id, ranks_per_node=64)
        job.save()
        job.refresh_from_db()
        t1 = job.last_update

        job.num_nodes *= 2
        job.save()
        job.refresh_from_db()
        assert job.num_nodes == 2
        print(t1, job.last_update)
        assert job.last_update > t1

    def test_can_view_history(self, client):
        App = client.App
        Site = client.Site
        Job = client.Job
        EventLog = client.EventLog
        site = Site.objects.create(name="theta", path="/projects/foo")
        app = App.objects.create(site_id=site.id, name="one", serialized_class="txt", source_code="txt")
        job = Job("test/test", app_id=app.id)
        job.save()
        history = EventLog.objects.filter(job_id=job.id)
        states = [event.to_state for event in history]
        assert states == ["STAGED_IN"]

        update_time = datetime.utcnow()
        job.state = "PREPROCESSED"
        job.state_data = {"message": "Skipped Preprocess: nothing to do"}
        job.state_timestamp = update_time
        Job.objects.bulk_update([job])

        assert job.state == "PREPROCESSED"
        latest_event = EventLog.objects.filter(job_id=job.id)[0]
        assert latest_event.to_state == "PREPROCESSED"
        assert latest_event.timestamp == update_time

    def test_bulk_delete(self, client):
        App = client.App
        Site = client.Site
        Job = client.Job
        site = Site.objects.create(name="theta", path="/projects/foo")
        app = App.objects.create(site_id=site.id, name="One", serialized_class="txt", source_code="txt")
        jobs = [Job(f"test/{i}", app_id=app.id) for i in range(3)]
        Job.objects.bulk_create(jobs)
        assert Job.objects.count() == 3
        Job.objects.all().delete()
        assert Job.objects.count() == 0

    def test_filter_by_tags(self, client):
        App = client.App
        Site = client.Site
        Job = client.Job
        site = Site.objects.create(name="theta", path="/projects/foo")
        app = App.objects.create(site_id=site.id, name="one", serialized_class="txt", source_code="txt")
        jobs = [Job(f"test/{i}", app_id=app.id, tags={"foo": i, "bar": i * 2}) for i in range(3)]
        Job.objects.bulk_create(jobs)
        assert Job.objects.count() == 3
        qs = Job.objects.filter(tags="foo:1")
        assert qs.count() == 1
        assert qs[0].workdir.as_posix() == "test/1"
        assert Job.objects.filter(tags=["foo:2", "bar:3"]).count() == 0
        assert Job.objects.filter(tags=["foo:2", "bar:4"]).count() == 1

    def test_filter_by_id_list(self, client):
        App = client.App
        Site = client.Site
        Job = client.Job
        site = Site.objects.create(name="theta", path="/projects/foo")
        app = App.objects.create(site_id=site.id, name="one", serialized_class="txt", source_code="txt")

        foo_jobs = [Job(f"foo/{i}", app_id=app.id) for i in range(3)]
        foo_jobs = Job.objects.bulk_create(foo_jobs)
        ids = [j.id for j in foo_jobs]

        bar_jobs = [Job(f"bar/{i}", app_id=app.id) for i in range(3)]
        bar_jobs = Job.objects.bulk_create(bar_jobs)

        assert Job.objects.count() == 6
        assert Job.objects.filter(id=ids).count() == 3

    def test_state_ordering(self, client):
        App = client.App
        Site = client.Site
        Job = client.Job
        site = Site.objects.create(name="theta", path="/projects/foo")
        app = App.objects.create(site_id=site.id, name="one", serialized_class="txt", source_code="txt")
        jobs = [Job(f"foo/{i}", app_id=app.id) for i in range(4)]
        jobs = Job.objects.bulk_create(jobs)
        jobs[2].state = "PREPROCESSED"
        jobs[2].save()

        states = [job.state for job in Job.objects.all().order_by("state")]
        assert states == ["PREPROCESSED"] + ["STAGED_IN"] * 3

    def test_filter_by_workdir(self, client):
        App = client.App
        Site = client.Site
        Job = client.Job
        site = Site.objects.create(name="theta", path="/projects/foo")
        app = App.objects.create(site_id=site.id, name="one", serialized_class="txt", source_code="txt")
        jobs = [Job(f"foo/{i}", app_id=app.id) for i in range(4)]
        jobs.append(Job("bar/99", app_id=app.id))
        jobs = Job.objects.bulk_create(jobs)

        assert Job.objects.filter(workdir__contains="foo/2").count() == 1
        assert Job.objects.filter(workdir__contains="foo/8").count() == 0
        assert Job.objects.filter(workdir__contains="foo").count() == 4
        assert Job.objects.filter(workdir__contains="bar").count() == 1

    def test_filter_by_site(self, client):
        App = client.App
        Site = client.Site
        Job = client.Job
        site1 = Site.objects.create(name="theta1", path="/projects/foo")
        site2 = Site.objects.create(name="theta2", path="/projects/bar")
        app1 = App.objects.create(site_id=site1.id, name="one", serialized_class="txt", source_code="txt")
        app2 = App.objects.create(site_id=site2.id, name="one", serialized_class="txt", source_code="txt")

        jobs = [Job(f"foo/{i}", app_id=app1.id) for i in range(2)] + [
            Job(f"foo/{i}", app_id=app2.id) for i in range(2, 4)
        ]
        jobs = Job.objects.bulk_create(jobs)

        # Check site filters
        assert Job.objects.filter(site_id=site1.id).count() == 2
        assert Job.objects.filter(site_id=site2.id).count() == 2
        assert Job.objects.all().count() == 4

    def test_create_by_name_and_site_path(self, client, appdef_a, appdef_b):
        App = client.App
        Site = client.Site
        Job = client.Job

        site1 = Site.objects.create(name="theta1", path="/projects/foo")
        AppA = appdef_a
        AppA.site = site1
        AppA.sync()

        site2 = Site.objects.create(name="theta2", path="/projects/bar")
        AppB = appdef_b
        AppB.site = site2
        AppB.sync()

        job1 = Job("test/1", app_id="AppA", site_name="theta1")
        job1.save()
        assert job1.id is not None
        assert job1.app_id == AppA.__app_id__

        job2 = Job("test/2", app_id="AppB", site_name="theta2")
        job2.save()
        assert job2.id is not None
        assert job2.app_id == AppB.__app_id__

        with pytest.raises(App.DoesNotExist):
            Job("test/2", app_id="AppB", site_name="theta3")

    def test_filter_by_state(self, client):
        App = client.App
        Site = client.Site
        Job = client.Job
        site = Site.objects.create(name="theta", path="/projects/foo")
        app = App.objects.create(site_id=site.id, name="one", serialized_class="txt", source_code="txt")

        jobs = [Job(f"foo/{i}", app_id=app.id) for i in range(4)]
        jobs = Job.objects.bulk_create(jobs)
        jobs[2].state = "PREPROCESSED"
        jobs[2].save()

        assert Job.objects.filter(state="STAGED_IN").count() == 3
        assert Job.objects.filter(state="PREPROCESSED").count() == 1
        assert Job.objects.filter(state__ne="STAGED_IN").count() == 1
        assert Job.objects.filter(state__ne="PREPROCESSED").count() == 3

    def test_filter_by_pending_cleanup(self, client):
        App = client.App
        Site = client.Site
        Job = client.Job
        site = Site.objects.create(name="theta", path="/projects/foo")
        app = App.objects.create(site_id=site.id, name="one", serialized_class="txt", source_code="txt")

        jobs = [Job(f"foo/{i}", app_id=app.id) for i in range(4)]
        jobs = Job.objects.bulk_create(jobs)

        pending_cleanup = Job.objects.filter(state="STAGED_IN", pending_file_cleanup=True)
        assert pending_cleanup.count() == 4

        for job in jobs:
            job.pending_file_cleanup = False
        Job.objects.bulk_update(jobs)

        pending_cleanup = Job.objects.filter(state="STAGED_IN", pending_file_cleanup=True)
        assert pending_cleanup.count() == 0


class TestTransfers:
    def create_app_with_transfers(self, client):
        site = client.Site.objects.create(
            name="theta",
            path="/projects/foo",
            transfer_locations={"laptop": f"globus://{uuid4()}"},
        )
        app = client.App.objects.create(
            site_id=site.id,
            name="one",
            transfers={
                "input_data": {
                    "required": True,
                    "direction": "in",
                    "local_path": "inp.dat",
                },
                "output_results": {
                    "required": False,
                    "direction": "out",
                    "local_path": "results.json",
                },
            },
            serialized_class="txt",
            source_code="txt",
        )
        return app

    def test_stage_in_flow(self, client):
        Job = client.Job
        TransferItem = client.TransferItem
        app = self.create_app_with_transfers(client)
        job = Job.objects.create(
            workdir="test/test",
            app_id=app.id,
            transfers={"input_data": {"location_alias": "laptop", "path": "/path/to/input.dat"}},
        )
        assert job.state == "READY"

        transfer = TransferItem.objects.get(job_id=job.id)
        transfer.task_id = "abc"
        transfer.state = "active"
        transfer.save()

        job.refresh_from_db()
        assert job.state == "READY"

        transfer.state = "done"
        transfer.save()
        job.refresh_from_db()
        assert job.state == "STAGED_IN"

        job.state = "POSTPROCESSED"
        job.save()
        job.refresh_from_db()
        assert job.state == "JOB_FINISHED"

    def test_stage_out_flow(self, client):
        Job = client.Job
        TransferItem = client.TransferItem
        app = self.create_app_with_transfers(client)
        job = Job.objects.create(
            workdir="test/test",
            app_id=app.id,
            transfers={
                "input_data": {
                    "location_alias": "laptop",
                    "path": "/path/to/input.dat",
                },
                "output_results": {
                    "location_alias": "laptop",
                    "path": "/path/to/output.json",
                },
            },
        )
        assert job.state == "READY"
        transfers = TransferItem.objects.filter(job_id=job.id)
        assert transfers.count() == 2
        stage_in = [t for t in transfers if t.direction == "in"][0]
        stage_out = [t for t in transfers if t.direction == "out"][0]
        assert TransferItem.objects.get(state="pending") == stage_in
        assert TransferItem.objects.get(state="awaiting_job") == stage_out

        stage_in.state = "done"
        stage_in.save()
        job.refresh_from_db()
        assert job.state == "STAGED_IN"

        job.state = "POSTPROCESSED"
        job.save()
        job.refresh_from_db()
        assert job.state == "POSTPROCESSED"
        stage_out.refresh_from_db()
        assert stage_out.state == "pending"

        stage_out.state = "done"
        stage_out.save()
        job.refresh_from_db()
        assert job.state == "JOB_FINISHED"

    def test_filter_transfers_by_state(self, client):
        Job = client.Job
        TransferItem = client.TransferItem
        app = self.create_app_with_transfers(client)
        Job.objects.create(
            workdir="test/test",
            app_id=app.id,
            transfers={
                "input_data": {
                    "location_alias": "laptop",
                    "path": "/path/to/input.dat",
                },
                "output_results": {
                    "location_alias": "laptop",
                    "path": "/path/to/output.json",
                },
            },
        )
        pending = TransferItem.objects.filter(state="pending")
        assert pending.count() == 1
        pending = TransferItem.objects.filter(state=TransferItemState.pending)
        assert pending.count() == 1
        done = TransferItem.objects.filter(state=TransferItemState.done)
        assert done.count() == 0
        all_t = TransferItem.objects.filter(state=["pending", "awaiting_job"])
        assert all_t.count() == 2
        all_t = TransferItem.objects.filter(state={"pending", "awaiting_job"})
        assert all_t.count() == 2

    def test_create_transfers_shortcut(self, client):
        Job = client.Job
        TransferItem = client.TransferItem
        app = self.create_app_with_transfers(client)
        Job.objects.create(
            workdir="test/test",
            app_id=app.id,
            transfers={
                "input_data": "laptop:/path/to/input.dat",
                "output_results": "laptop:/path/to/output.json",
            },
        )
        pending = TransferItem.objects.filter(state="pending")
        assert pending.count() == 1
        assert pending[0].remote_path.as_posix() == "/path/to/input.dat"


class TestEvents:
    def setup_scenario(self, client):
        Site = client.Site
        App = client.App
        Job = client.Job
        site = Site.objects.create(name="theta", path="/projects/foo")
        app = App.objects.create(site_id=site.id, name="one", serialized_class="txt", source_code="txt")
        before_creation = datetime.utcnow()

        j1 = Job.objects.create(workdir="foo/1", app_id=app.id)
        j2 = Job.objects.create(workdir="foo/2", app_id=app.id)
        j3 = Job.objects.create(workdir="foo/3", app_id=app.id)

        j1.state = "PREPROCESSED"
        j1.save()

        j2.state = "PREPROCESSED"
        j2.save()
        j2.state = "RUNNING"
        j2.save()
        j2.state = "RUN_ERROR"
        j2.state_timestamp = datetime.utcnow() + timedelta(minutes=1)
        j2.save()

        j3.state = "PREPROCESSED"
        j3.save()
        j3.state = "RUNNING"
        j3.save()
        j3.state = "RUN_DONE"
        j3.state_data = {"message": "OK: done!"}
        j3.save()
        return before_creation

    def test_filter_by_job(self, client):
        self.setup_scenario(client)
        Job = client.Job
        EventLog = client.EventLog
        id = Job.objects.get(workdir__contains="foo/2").id
        qs = EventLog.objects.filter(job_id=id)
        states = [event.to_state for event in qs]
        assert states == ["RUN_ERROR", "RUNNING", "PREPROCESSED", "STAGED_IN"]

    def test_filter_by_to_state(self, client):
        self.setup_scenario(client)
        EventLog = client.EventLog
        assert EventLog.objects.filter(to_state="RUN_ERROR").count() == 1
        assert EventLog.objects.filter(to_state="STAGED_IN").count() == 3

    def test_filter_by_from_state(self, client):
        self.setup_scenario(client)
        EventLog = client.EventLog
        assert EventLog.objects.filter(from_state="CREATED").count() == 3

    def test_filter_by_to_and_from_state(self, client):
        self.setup_scenario(client)
        EventLog = client.EventLog
        qs = EventLog.objects.filter(from_state="RUNNING").filter(to_state="RUN_ERROR")
        assert qs.count() == 1
        assert qs[0] == EventLog.objects.get(from_state="RUNNING", to_state="RUN_ERROR")

    def test_filter_by_message(self, client):
        self.setup_scenario(client)
        EventLog = client.EventLog
        qs = EventLog.objects.filter(data="message:OK: done!")
        assert qs.count() == 1
        assert qs[0].to_state == "RUN_DONE"

    def test_filter_by_timestamp_range(self, client):
        self.setup_scenario(client)
        EventLog = client.EventLog
        t = datetime.utcnow() + timedelta(seconds=30)
        assert EventLog.objects.filter(timestamp_after=t).count() == 1

    def test_cannot_create_or_update(self, client):
        self.setup_scenario(client)
        EventLog = client.EventLog
        with pytest.raises(AttributeError, match="has no attribute 'create'"):
            EventLog.objects.create(
                job_id=1,
                from_state="RUNNING",
                to_state="RUN_DONE",
            )
        log = EventLog.objects.first()
        with pytest.raises(AttributeError, match="EventLog is read-only"):
            log.from_state = "CREATED"
        log.save()


class TestBatchJobs:
    def test_create(self, client):
        Site = client.Site
        BatchJob = client.BatchJob
        site = Site.objects.create(name="theta", path="/projects/foo")
        bjob = BatchJob.objects.create(
            site_id=site.id,
            project="datascience",
            queue="default",
            num_nodes=128,
            wall_time_min=30,
            job_mode="mpi",
            filter_tags={"system": "H2O", "calc_type": "energy"},
        )
        assert bjob.state == "pending_submission"
        assert bjob.id is not None

    def test_filter_by_scheduler_id(self, client):
        Site = client.Site
        BatchJob = client.BatchJob
        site = Site.objects.create(name="theta", path="/projects/foo")
        bjob = BatchJob.objects.create(
            site_id=site.id,
            project="datascience",
            queue="default",
            num_nodes=128,
            wall_time_min=30,
            job_mode="mpi",
            filter_tags={"system": "H2O", "calc_type": "energy"},
        )
        bjob.scheduler_id = 2468
        bjob.save()
        from_db = BatchJob.objects.get(site_id=site.id, scheduler_id=2468)
        assert from_db.id == bjob.id
        assert bjob.id is not None

    def test_filter_by_site_id(self, client):
        Site = client.Site
        BatchJob = client.BatchJob
        site1 = Site.objects.create(name="theta1", path="/projects/foo")
        site2 = Site.objects.create(name="theta2", path="/projects/bar")
        assert site1.id != site2.id
        BatchJob.objects.create(
            site_id=site1.id,
            project="datascience",
            queue="default",
            num_nodes=128,
            wall_time_min=30,
            job_mode="mpi",
        )
        BatchJob.objects.create(
            site_id=site2.id,
            project="datascience",
            queue="default",
            num_nodes=128,
            wall_time_min=30,
            job_mode="mpi",
        )
        from_db = BatchJob.objects.filter(site_id=site1.id)
        assert len(from_db) == 1 and from_db[0].site_id == site1.id
        from_db = BatchJob.objects.filter(site_id=site2.id)
        assert len(from_db) == 1 and from_db[0].site_id == site2.id

    def test_bulk_update(self, client):
        Site = client.Site
        BatchJob = client.BatchJob
        site = Site.objects.create(name="theta", path="/projects/foo")
        for i in range(3):
            BatchJob.objects.create(
                site_id=site.id,
                project="datascience",
                queue="default",
                num_nodes=128,
                wall_time_min=30,
                job_mode="mpi",
                filter_tags={"system": "H2O", "calc_type": "energy"},
            )

        assert BatchJob.objects.count() == 3

        bjobs = BatchJob.objects.filter(site_id=site.id)
        for job, sched_id in zip(bjobs, [123, 124, 125]):
            job.state = "queued"
            job.scheduler_id = sched_id
        BatchJob.objects.bulk_update(bjobs)

        after_update = list(BatchJob.objects.filter(site_id=site.id))
        assert after_update == list(bjobs)

    def test_delete(self, client):
        Site = client.Site
        BatchJob = client.BatchJob
        site = Site.objects.create(name="theta", path="/projects/foo")
        for i in range(3):
            BatchJob.objects.create(
                site_id=site.id,
                project="datascience",
                queue="default",
                num_nodes=128,
                wall_time_min=30,
                job_mode="mpi",
                filter_tags={"system": "H2O", "calc_type": "energy"},
            )
        assert BatchJob.objects.count() == 3

        with pytest.raises(NotImplementedError):
            BatchJob.objects.filter(site_id=site.id).delete()

        for job in BatchJob.objects.all():
            job.delete()
        assert BatchJob.objects.count() == 0

    def test_filter_by_tags(self, client):
        Site = client.Site
        BatchJob = client.BatchJob
        site = Site.objects.create(name="theta", path="/projects/foo")
        for system in ["H2O", "D2O", "NH3", "CH2O"]:
            BatchJob.objects.create(
                site_id=site.id,
                project="datascience",
                queue="default",
                num_nodes=128,
                wall_time_min=30,
                job_mode="mpi",
                filter_tags={"system": system, "calc_type": "energy"},
            )

        assert BatchJob.objects.filter(filter_tags="system:NH3").count() == 1

    def test_fetch_associated_jobs(self, client):
        Site = client.Site
        BatchJob = client.BatchJob
        App = client.App
        Job = client.Job
        Session = client.Session
        site = Site.objects.create(name="theta", path="/projects/foo")
        app = App.objects.create(site_id=site.id, name="one", serialized_class="txt", source_code="txt")

        batch_job = BatchJob.objects.create(
            site_id=site.id,
            project="datascience",
            queue="default",
            num_nodes=128,
            wall_time_min=30,
            job_mode="mpi",
        )
        batch_job.scheduler_id = 1234
        batch_job.state = "running"
        batch_job.save()

        for i in range(3):
            job = Job.objects.create(workdir=f"test/{i}", app_id=app.id)
            job.state = "PREPROCESSED"
            job.save()
            assert job.batch_job_id is None

        sess = Session.objects.create(batch_job_id=batch_job.id, site_id=batch_job.site_id)
        acquired = sess.acquire_jobs(
            max_wall_time_min=60,
            max_nodes_per_job=8,
            max_num_jobs=8,
            filter_tags={},
        )
        assert len(acquired) == 3

        related = Job.objects.filter(batch_job_id=batch_job.id)
        assert sorted([j.id for j in related]) == sorted([j.id for j in acquired])


class TestSessions:
    def create_site_app(self, client):
        Site = client.Site
        App = client.App
        site = Site.objects.create(name="theta", path="/projects/foo")
        app = App.objects.create(site_id=site.id, name="one", serialized_class="txt", source_code="txt")
        return site, app

    def job(self, client, name, app, args={}, **kwargs):
        return client.Job(f"test/{name}", app_id=app.id, parameters=args, **kwargs)

    def create_jobs(self, client, app, num_jobs=3, state="PREPROCESSED"):
        jobs = [self.job(client, i, app) for i in range(num_jobs)]
        jobs = client.Job.objects.bulk_create(jobs)
        for job in jobs:
            job.state = state
        client.Job.objects.bulk_update(jobs)

    def create_sess(self, client, site):
        batch_job = client.BatchJob.objects.create(
            site_id=site.id,
            project="datascience",
            queue="default",
            num_nodes=128,
            wall_time_min=30,
            job_mode="mpi",
        )
        sess = client.Session.objects.create(batch_job_id=batch_job.id, site_id=site.id)
        return sess

    def test_create(self, client):
        before_create = datetime.utcnow()
        site = client.Site.objects.create(name="theta", path="/projects/foo")
        sess = self.create_sess(client, site)
        assert sess.heartbeat > before_create

    def test_acquire(self, client):
        site, app = self.create_site_app(client)
        self.create_jobs(client, app, num_jobs=3)
        sess = self.create_sess(client, site)

        acquired = sess.acquire_jobs(
            max_wall_time_min=60,
            max_nodes_per_job=8,
            max_num_jobs=8,
            filter_tags={},
        )
        assert len(acquired) == 3
        for job in acquired:
            assert job.batch_job_id > 0

    def test_acquire_by_app_ids(self, client):
        site, app1 = self.create_site_app(client)
        app2 = client.App.objects.create(site_id=site.id, name="two", serialized_class="txt", source_code="txt")
        self.create_jobs(client, app1, num_jobs=5)
        self.create_jobs(client, app2, num_jobs=5)
        sess = self.create_sess(client, site)

        acquired = sess.acquire_jobs(
            max_num_jobs=20,
            app_ids={app2.id},
        )
        assert len(acquired) == 5
        for job in acquired:
            assert job.app_id == app2.id

    def test_acquire_for_preprocessing(self, client):
        site, app = self.create_site_app(client)
        self.create_jobs(client, app, num_jobs=10, state="STAGED_IN")
        sess = client.Session.objects.create(site_id=site.id)
        assert sess.batch_job_id is None

        acquired = sess.acquire_jobs(
            max_wall_time_min=3600,
            max_nodes_per_job=4096,
            max_num_jobs=100,
            states=["STAGED_IN", "RUN_DONE", "RUN_ERROR", "RUN_TIMEOUT"],
        )
        assert len(acquired) == 10
        assert all(j.state == "STAGED_IN" for j in acquired)

    def test_acquire_with_filter_tags(self, client):
        site, app = self.create_site_app(client)

        job_tags = [
            dict(system="H2O", type="energy"),
            dict(system="D2O", type="energy"),
            dict(system="NO2", type="energy"),
            dict(system="H2O", type="gradient"),
        ]
        jobs = [self.job(client, i, app, tags=tags) for i, tags in enumerate(job_tags)]
        jobs = client.Job.objects.bulk_create(jobs)
        client.Job.objects.all().update(state="PREPROCESSED")

        sess = self.create_sess(client, site)
        acquired = sess.acquire_jobs(
            max_wall_time_min=60,
            max_nodes_per_job=8,
            max_num_jobs=8,
            filter_tags={"system": "H2O", "type": "energy"},
        )
        assert len(acquired) == 1
        assert acquired[0].tags == dict(system="H2O", type="energy")

    def test_tick(self, client):
        site = client.Site.objects.create(name="theta", path="/projects/foo")
        sess = self.create_sess(client, site)
        creation_time = sess.heartbeat
        sess.tick()
        sess.refresh_from_db()
        assert sess.heartbeat > creation_time

    def test_delete(self, client):
        site, app = self.create_site_app(client)
        self.create_jobs(client, app, num_jobs=3)

        sess = self.create_sess(client, site)
        acquired = sess.acquire_jobs(
            max_wall_time_min=60,
            max_nodes_per_job=8,
            max_num_jobs=8,
        )
        assert len(acquired) == 3
        assert sess.id is not None
        sess.delete()

        # After deleting session, the jobs can be re-acquired:
        assert client.Session.objects.all().count() == 0
        sess2 = self.create_sess(client, site)
        acquired = sess2.acquire_jobs(
            max_wall_time_min=60,
            max_nodes_per_job=8,
            max_num_jobs=8,
        )
        assert len(acquired) == 3
