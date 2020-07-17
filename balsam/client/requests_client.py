import logging
import requests
from rest_framework import status
from pprint import pformat
from json import JSONDecodeError

from .rest_base_client import RESTClient

logger = logging.getLogger(__name__)


class RequestsClient(RESTClient):
    def __init__(self, base_url, connect_timeout=3.1, read_timeout=60, retry_count=3):
        super().__init__(base_url)
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.retry_count = retry_count
        self._session = requests.Session()

    def request(self, url, http_method, params=None, json=None, data=None):
        absolute_url = self.base_url.rstrip("/") + "/" + url.lstrip("/")
        attempt = 0
        tried_reauth = False
        while attempt < self.retry_count:
            try:
                print("Client:", http_method, absolute_url)
                response = self._do_request(
                    absolute_url, http_method, params, json, data
                )
            except requests.Timeout as exc:
                print("Timeout; retrying...")
                attempt += 1
                if attempt == self.retry_count:
                    raise requests.Timeout(f"Timed-out {attempt} times.") from exc
            except requests.HTTPError as exc:
                print("HTTPError")
                if (
                    exc.response.status_code != status.HTTP_401_UNAUTHORIZED
                    or tried_reauth
                ):
                    raise
                self.refresh_auth()
                tried_reauth = True
            else:
                try:
                    return response.json()
                except JSONDecodeError:
                    if http_method != "DELETE":
                        raise
                    return None

    def _do_request(self, absolute_url, http_method, params, json, data):
        response = self._session.request(
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
