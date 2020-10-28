import os
import logging
import requests
from pprint import pformat
from json import JSONDecodeError

from .rest_base_client import RESTClient

logger = logging.getLogger(__name__)


class NotAuthenticatedError(Exception):
    pass


class RequestsClient(RESTClient):
    def __init__(self, api_root, connect_timeout=3.1, read_timeout=60, retry_count=3):
        self.api_root = api_root
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.retry_count = retry_count
        self._session = None
        self._pid = os.getpid()
        self._authenticated = False
        self.token = None

    @property
    def session(self):
        pid = os.getpid()
        if pid != self._pid or self._session is None:
            self._session = requests.Session()
            self._pid = pid
            if self.token:
                self._session.headers["Authorization"] = f"Bearer {self.token}"
        return self._session

    def close_session(self):
        self._session = None

    def request(
        self, url, http_method, params=None, json=None, data=None, authenticating=False
    ):
        if not self._authenticated and not authenticating:
            raise NotAuthenticatedError(
                "Cannot perform unauthenticated request. Please login with `balsam login`"
            )
        absolute_url = self.api_root.rstrip("/") + "/" + url.lstrip("/")
        attempt = 0
        while attempt < self.retry_count:
            try:
                logger.debug(f"{http_method}: {absolute_url}")
                response = self._do_request(
                    absolute_url, http_method, params, json, data
                )
            except requests.Timeout as exc:
                logger.warning(f"Timed out request {http_method} {absolute_url}")
                attempt += 1
                if attempt == self.retry_count:
                    raise requests.Timeout(f"Timed-out {attempt} times.") from exc
            else:
                try:
                    return response.json()
                except JSONDecodeError:
                    if http_method != "DELETE":
                        raise
                    return None

    def _do_request(self, absolute_url, http_method, params, json, data):
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

    def _raise_with_explanation(self, response):
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
