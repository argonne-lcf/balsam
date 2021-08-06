import logging
import os
import random
import time
from json import JSONDecodeError
from pprint import pformat
from typing import Any, Dict, List, Optional, Type, Union

import requests

from . import urls
from .rest_base_client import RESTClient

logger = logging.getLogger(__name__)

OptionalAnyJSON = Optional[Union[Dict[str, Any], List[Any]]]


class NotAuthenticatedError(Exception):
    pass


class RequestsClient(RESTClient):

    _client_classes: "Dict[str, Type[RequestsClient]]" = {}

    @staticmethod
    def discover_supported_client(base_url: str) -> "RequestsClient":
        url = base_url.rstrip("/") + "/" + urls.CHECK_LOGIN_FLOWS
        auth_methods: List[str] = requests.get(url).json()

        if "LoginMethod.oauth_device" in auth_methods:
            cls = RequestsClient._client_classes["oauth_device"]
        elif "LoginMethod.password" in auth_methods:
            cls = RequestsClient._client_classes["password"]
        else:
            raise NotImplementedError(f"This client does not support the server's auth methods: {auth_methods}")

        return cls(api_root=base_url)

    def __init__(
        self, api_root: str, connect_timeout: float = 3.1, read_timeout: float = 120.0, retry_count: int = 3
    ) -> None:
        self.api_root = api_root
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.retry_count = retry_count
        self._session: Optional[requests.Session] = None
        self._pid = os.getpid()
        self._authenticated = False
        self.token: Optional[str] = None
        self._attempt: int = 0

    @property
    def session(self) -> requests.Session:
        """
        !!! WARNING !!!
        requests.Session is not multiprocessing-safe. You will get
        crossed-wires and mixed up responses; leading to strange and
        near-impossible-to-debug issues. As an extra precaution here we start
        a new Session if a PID change is detected.
        """
        pid = os.getpid()
        if pid != self._pid or self._session is None:
            self._session = requests.Session()
            self._pid = pid
            if self.token:
                self._session.headers["Authorization"] = f"Bearer {self.token}"
        return self._session

    def close_session(self) -> None:
        self._session = None
        return None

    def backoff(self, reason: Exception) -> None:
        if self._attempt > self.retry_count:
            raise TimeoutError(f"Exceeded max retries: {reason}")
        sleep_time = 2 ** self._attempt + random.random()
        time.sleep(sleep_time)
        self._attempt += 1

    def request(
        self,
        url: str,
        http_method: str,
        params: Optional[Dict[str, Any]] = None,
        json: OptionalAnyJSON = None,
        data: OptionalAnyJSON = None,
        authenticating: bool = False,
    ) -> OptionalAnyJSON:
        if not self._authenticated and not authenticating:
            raise NotAuthenticatedError("Cannot perform unauthenticated request. Please login with `balsam login`")
        absolute_url = self.api_root.rstrip("/") + "/" + url.lstrip("/")
        self._attempt = 0
        while True:
            try:
                logger.debug(f"{http_method}: {absolute_url} {params if params else ''}")
                response = self._do_request(absolute_url, http_method, params, json, data)
            except requests.Timeout as exc:
                logger.warning(f"Attempt Retry of Timed-out request {http_method} {absolute_url}")
                self.backoff(exc)
            except requests.ConnectionError as exc:
                logger.warning(f"Attempt retry of connection: {exc}")
                self.backoff(exc)
            else:
                try:
                    return response.json()  # type: ignore
                except (ValueError, JSONDecodeError):
                    if http_method != "DELETE":
                        raise
                    return None

    def _do_request(
        self,
        absolute_url: str,
        http_method: str,
        params: Optional[Dict[str, Any]],
        json: OptionalAnyJSON,
        data: OptionalAnyJSON,
    ) -> requests.Response:
        response = self.session.request(
            http_method,
            url=absolute_url,
            params=params,
            json=json,
            data=data,
            timeout=(self.connect_timeout, self.read_timeout),
        )
        if response.status_code >= 400:
            self._raise_with_explanation(response)
        return response

    def _raise_with_explanation(self, response: requests.Response) -> None:
        """
        Add the API's informative error message to Requests' generic status Exception
        """
        try:
            explanation = response.json()
        except JSONDecodeError:
            explanation = ""
        else:
            explanation = "\n" + pformat(explanation, indent=4)

        if response.reason is None:
            response.reason = explanation
        elif isinstance(response.reason, bytes):
            response.reason = response.reason.decode("utf-8") + explanation
        else:
            response.reason += explanation
        response.raise_for_status()
