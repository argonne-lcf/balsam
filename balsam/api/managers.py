from .manager_base import Manager
from .models import (
    Job,
    Site,
    App,
    BatchJob,
    Session,
    EventLog,
)


class JobManager(Manager):
    model_class = Job
    bulk_create_enabled = True
    bulk_update_enabled = True
    bulk_delete_enabled = True

    def _to_dict(self, instance):
        exclude_keys = {
            "job",
            "protocol",
            "remote_netloc",
            "source_path",
            "destination_path",
        }

        transfer_item_excludes = {
            idx: exclude_keys for idx in range(len(instance.transfer_items))
        }
        d = instance.dict(
            exclude={
                "_children": ...,
                "_batch_job": ...,
                "_app_name": ...,
                "_site": ...,
                "_app_class": ...,
                "_lock_status": ...,
                "_last_update": ...,
                "transfer_items": transfer_item_excludes,
            }
        )

        return self._make_encodable(d)

    def create(
        self,
        workdir,
        app,
        parameters,
        parents=None,
        transfer_items=None,
        tags=None,
        data=None,
        num_nodes=1,
        ranks_per_node=1,
        threads_per_rank=1,
        threads_per_core=1,
        cpu_affinity="",
        gpus_per_rank=0,
        node_packing_count=1,
        wall_time_min=0,
        **kwargs,
    ):
        d = {k: v for k, v in locals().items() if v is not None and k != "self"}
        for k, v in kwargs.items():
            if v is not None:
                d[k] = v
        return super().create(**d)


class SiteManager(Manager):
    model_class = Site


class AppManager(Manager):
    model_class = App

    def merge(self, app_list, name, description=""):
        pks = [app.pk for app in app_list]
        resp = self.resource.merge(
            name=name, description=description, existing_apps=pks
        )
        return self._from_dict(resp)


class BatchJobManager(Manager):
    model_class = BatchJob
    bulk_update_enabled = True


class SessionManager(Manager):
    model_class = Session

    def _do_acquire(self, instance, **kwargs):
        acquired_raw = self.resource.acquire_jobs(uri=instance.pk, **kwargs)
        acquired_job_list = acquired_raw["acquired_jobs"]
        return Job.objects._unpack_list_response(acquired_job_list)


class EventLogManager(Manager):
    model_class = EventLog
