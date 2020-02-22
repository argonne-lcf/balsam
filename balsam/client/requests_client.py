import requests
from rest_framework import status
from pprint import pformat
from json import JSONDecodeError

from .rest_base_client import RESTClient


class RequestsClient(RESTClient):
    def __init__(self, api_root, connect_timeout=3.1, read_timeout=5, retry_count=3):
        super().__init__(api_root)
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.retry_count = retry_count
        self._session = requests.Session()

    def request(self, absolute_url, http_method, payload=None):
        attempt = 0
        tried_reauth = False
        while attempt < self.retry_count:
            try:
                response = self._do_request(absolute_url, http_method, payload)
            except requests.Timeout as exc:
                attempt += 1
                if attempt == self.retry_count:
                    raise requests.Timeout(f"Timed-out {attempt} times.") from exc
            except requests.HTTPError as exc:
                if (
                    exc.response.status_code != status.HTTP_401_UNAUTHORIZED
                    or tried_reauth
                ):
                    raise
                self.refresh_auth()
                tried_reauth = True
            else:
                return response

    def _do_request(self, absolute_url, http_method, payload=None):
        response = self._session.request(
            http_method,
            url=absolute_url,
            json=payload,
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

    def extract_data(self, response):
        """
        Return response payload
        """
        return response.json()
