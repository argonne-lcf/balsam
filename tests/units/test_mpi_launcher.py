from datetime import datetime

import pytest

from balsam._api.models import Job
from balsam.platform.app_run import MPICHRun
from balsam.platform.compute_node import ThetaGPUNode
from balsam.site import ApplicationDefinition
from balsam.site.launcher._mpi_mode import Launcher
from balsam.site.launcher.node_manager import NodeManager


class HelloWorld(ApplicationDefinition):
    command_template = "echo Hello, {{name}}"
    parameters = {}


@pytest.fixture(scope="function")
def launcher(mocker, tmp_path):
    mocker.patch("balsam.platform.app_run.app_run.SubprocessAppRun", autospec=True)
    mock_job_source = mocker.patch("balsam.site.job_source.SynchronousJobSource", autospec=True)
    mock_status_updater = mocker.patch("balsam.site.status_updater.BulkStatusUpdater", autospec=True)
    mock_client = mocker.patch("balsam.client.BasicAuthRequestsClient", autospec=True)
    client = mock_client("http://test:1234", "testuser", token="foo")

    nodes = [ThetaGPUNode(nid, "thetagpu{nid:02d}") for nid in range(1, 12)]
    node_manager = NodeManager(nodes, allow_node_packing=True)

    app_cache = {1: HelloWorld}
    mock_job_source.get_jobs.return_value = [
        Job(
            _api_data=True,
            id=i,
            workdir=f"test/{i}",
            app_id=1,
            state="PREPROCESSED",
            last_update=datetime.utcnow(),
            pending_file_cleanup=True,
        )
        for i in range(10)
    ]

    job_source = mock_job_source(
        client=client,
        site_id=123,
        filter_tags={},
        max_wall_time_min=60,
        scheduler_id=25,
        app_ids={app_id for app_id in app_cache if app_id is not None},
    )
    status_updater = mock_status_updater(client)
    return Launcher(
        data_dir=tmp_path,
        idle_ttl_sec=10,
        delay_sec=0.01,
        app_cache=app_cache,
        app_run=MPICHRun,
        node_manager=node_manager,
        job_source=job_source,
        status_updater=status_updater,
        wall_time_min=60,
        error_tail_num_lines=10,
        max_concurrent_runs=1000,
    )


def test_launcher_foo(launcher, tmp_path):
    launcher.launch_runs()
