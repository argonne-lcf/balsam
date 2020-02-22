import requests
from rest_framework import status
from pprint import pformat
from json import JSONDecodeError

from .rest_base_client import RESTClient


class BasicAuthForTokenMixin:
    def refresh_auth(self):
        # Login with HTTPBasic Auth to get a token:
        self._session.auth = ("misha", "f")
        login_url = self.build_url("login")
        resp = self._session.post(login_url)
        token = resp.json()["token"]

        # Unset BasicAuth; set Token Authorization header
        self._session.auth = None
        self._session.headers["Authorization"] = f"Token {token}"

    def interactive_login(self):
        """Initiate interactive login flow"""
        raise NotImplementedError


class RequestsClient(BasicAuthForTokenMixin, RESTClient):
    CONNECT_TIMEOUT = 3.1
    READ_TIMEOUT = 5
    RETRY_COUNT = 3

    def __init__(self):
        super().__init__()
        self._session = requests.Session()

    def request(self, absolute_url, http_method, payload=None):
        attempt = 0
        tried_reauth = False
        while attempt < self.RETRY_COUNT:
            try:
                response = self._do_request(absolute_url, http_method, payload)
            except requests.Timeout as exc:
                attempt += 1
                if attempt == self.RETRY_COUNT:
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
            timeout=(self.CONNECT_TIMEOUT, self.READ_TIMEOUT),
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
