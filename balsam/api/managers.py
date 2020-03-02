from .query import Manager
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
        exclude_keys = [
            "job",
            "protocol",
            "remote_netloc",
            "source_path",
            "destination_path",
        ]

        # BUG: the exclude functionality in Pydantic 1.4
        # prevents nested exclude from working correctly,
        # so we do the exclusion ourselves here:
        d = instance.dict(
            exclude={
                "_children"
                "_batch_job"
                "_app_name"
                "_site"
                "_app_class"
                "_lock_status"
                "_last_update"
            }
        )
        for transfer_item in d["transfer_items"]:
            for key in exclude_keys:
                del transfer_item[key]

        return self._make_encodable(d)


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
        return Job.objects._unpack_list_response(acquired_raw)


class EventLogManager(Manager):
    model_class = EventLog
