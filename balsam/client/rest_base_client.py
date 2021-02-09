from datetime import timedelta
from typing import Any, Dict, List, Optional, Type

from balsam._api.models import (
    App,
    AppManager,
    BatchJob,
    BatchJobManager,
    EventLog,
    EventLogManager,
    Job,
    JobManager,
    Session,
    SessionManager,
    Site,
    SiteManager,
    TransferItem,
    TransferItemManager,
)

from .encoders import jsonable_encoder


class AuthError(Exception):
    pass


class RESTClient:
    expires_in: timedelta

    def __init__(*args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def interactive_login(self) -> Any:
        """Initiate interactive login flow"""
        raise NotImplementedError

    def refresh_auth(self) -> None:
        """
        Reload credentials if stored/not expired.
        Set appropriate Auth headers on HTTP session.
        """
        raise NotImplementedError

    def close_session(self) -> None:
        raise NotImplementedError

    def request(
        self,
        url: str,
        http_method: str,
        params: Optional[Dict[str, Any]] = None,
        json: Any = None,
        data: Any = None,
        authenticating: bool = False,
    ) -> Any:
        """
        Supports timeout retry, auto re-authentication, accepting DUPLICATE status
        Raises helfpul errors on 4**, 5**, TimeoutErrors, AuthErrors
        """
        raise NotImplementedError

    def get(self, url: str, **kwargs: Any) -> Any:
        """GET kwargs become URL query parameters (e.g. /?site=3)"""
        return self.request(url, "GET", params=kwargs)

    def post_form(self, url: str, **kwargs: Any) -> Any:
        return self.request(url, "POST", data=kwargs)

    def post(self, url: str, authenticating: bool = False, **kwargs: Any) -> Any:
        return self.request(url, "POST", json=jsonable_encoder(kwargs), authenticating=authenticating)

    def bulk_post(self, url: str, list_data: List[Any]) -> Any:
        return self.request(url, "POST", json=jsonable_encoder(list_data))

    def put(self, url: str, **kwargs: Any) -> Any:
        return self.request(url, "PUT", json=jsonable_encoder(kwargs))

    def bulk_put(self, url: str, payload: Any, **kwargs: Any) -> Any:
        return self.request(url, "PUT", json=jsonable_encoder(payload), params=kwargs)

    def patch(self, url: str, **kwargs: Any) -> Any:
        return self.request(url, "PATCH", json=jsonable_encoder(kwargs))

    def bulk_patch(self, url: str, list_data: List[Any]) -> Any:
        return self.request(url, "PATCH", json=jsonable_encoder(list_data))

    def delete(self, url: str) -> Any:
        return self.request(url, "DELETE")

    def bulk_delete(self, url: str, **kwargs: Any) -> Any:
        return self.request(url, "DELETE", params=kwargs)

    @property
    def Site(self) -> Type[Site]:
        return SiteManager(client=self)._model_class

    @property
    def App(self) -> Type[App]:
        return AppManager(client=self)._model_class

    @property
    def BatchJob(self) -> Type[BatchJob]:
        return BatchJobManager(client=self)._model_class

    @property
    def Job(self) -> Type[Job]:
        return JobManager(client=self)._model_class

    @property
    def TransferItem(self) -> Type[TransferItem]:
        return TransferItemManager(client=self)._model_class

    @property
    def Session(self) -> Type[Session]:
        return SessionManager(client=self)._model_class

    @property
    def EventLog(self) -> Type[EventLog]:
        return EventLogManager(client=self)._model_class
