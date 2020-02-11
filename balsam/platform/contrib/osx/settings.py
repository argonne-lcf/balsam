# flake8: noqa
from pathlib import Path
from balsam.platform.mpirun import DirectRun
from balsam.client.postgres_client import PostgresDjangoORMClient

here = Path(__file__).resolve().parent
MPIRun = DirectRun


def client():
    return PostgresDjangoORMClient.from_yaml(here)


Scheduler = LocalScheduler()
NodeSpec = PersonalNode(num_cores=4)
JobTemplate = DiscoverTemplates(here)


class Service:
    """
    Base class:
        - manages child service threads: - queue submission
            - transitions (pre and post)
            - transfer agent (globus batching)
            - queue & data cleanup
            - job source
            - status updater
            - sync site data (e.g. current node availability) with server
        - generic run loops for threads
    Use Mixins to control service thread behaviors:
        GreedyBackfillerMixin
        SteadyStateSubmitMixin
    Or directly set class attributes and override methods:
        get_runnable_tasks()
        get_submission_windows()
        get_next_submission()
    """

    transition_threads = 5
    job_source = None
    status_updater = ZeroMQ_Updater(host="thetalogin2", port=12345)
    transfer_agent = GlobusBatchTransferAgent(
        "alcf#dtn_theta", max_active=3, max_batch=1000, idle_wait_period=5,
    )
    submit_interval = None
    delete_queued_interval = None
    data_cleanup_interval = None
