from fastapi.encoders import jsonable_encoder
from balsam.api.models import (
    SiteManager,
    AppManager,
    BatchJobManager,
    JobManager,
    TransferManager,
    SessionManager,
    EventLogManager,
)


class AuthError(Exception):
    pass


class RESTClient:
    def interactive_login(self):
        """Initiate interactive login flow"""
        raise NotImplementedError

    def refresh_auth(self):
        """
        Reload credentials if stored/not expired.
        Set appropriate Auth headers on HTTP session.
        """
        raise NotImplementedError

    def request(
        self, url, http_method, params=None, json=None, data=None, authenticating=False
    ):
        """
        Supports timeout retry, auto re-authentication, accepting DUPLICATE status
        Raises helfpul errors on 4**, 5**, TimeoutErrors, AuthErrors
        """
        raise NotImplementedError

    def get(self, url, **kwargs):
        """GET kwargs become URL query parameters (e.g. /?site=3)"""
        return self.request(url, "GET", params=kwargs)

    def post_form(self, url, **kwargs):
        return self.request(url, "POST", data=kwargs)

    def post(self, url, authenticating=False, **kwargs):
        return self.request(
            url, "POST", json=jsonable_encoder(kwargs), authenticating=authenticating
        )

    def bulk_post(self, url, list_data):
        return self.request(url, "POST", json=jsonable_encoder(list_data))

    def put(self, url, **kwargs):
        return self.request(url, "PUT", json=jsonable_encoder(kwargs))

    def bulk_put(self, url, payload, **kwargs):
        return self.request(url, "PUT", json=jsonable_encoder(payload), params=kwargs)

    def patch(self, url, **kwargs):
        return self.request(url, "PATCH", json=jsonable_encoder(kwargs))

    def bulk_patch(self, url, list_data):
        return self.request(url, "PATCH", json=jsonable_encoder(list_data))

    def delete(self, url):
        return self.request(url, "DELETE")

    def bulk_delete(self, url, **kwargs):
        return self.request(url, "DELETE", params=kwargs)

    @property
    def Site(self):
        return SiteManager(client=self).model_class

    @property
    def App(self):
        return AppManager(client=self).model_class

    @property
    def BatchJob(self):
        return BatchJobManager(client=self).model_class

    @property
    def Job(self):
        return JobManager(client=self).model_class

    @property
    def Transfer(self):
        return TransferManager(client=self).model_class

    @property
    def Session(self):
        return SessionManager(client=self).model_class

    @property
    def EventLog(self):
        return EventLogManager(client=self).model_class
