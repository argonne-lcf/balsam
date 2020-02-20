from rest_framework import status
from urllib.parse import urlencode, urljoin
import requests
from pprint import pformat
from json import JSONDecodeError


class Client:
    API_SERVER = "http://localhost:8000"
    API_VERSION_ROOT = "api"

    def __init__(self):
        self.API_VERSION_ROOT = self.API_VERSION_ROOT.strip("/")
        self.jobs = JobResource(self, "jobs/")

    def build_url(self, url, **query_params):
        result = urljoin(self.API_SERVER, self.API_VERSION_ROOT + "/" + url.lstrip("/"))
        if query_params:
            result += "?" + urlencode(query_params)
        return result

    def interactive_login(self):
        """Initiate interactive login flow"""
        raise NotImplementedError

    def refresh_auth(self):
        """
        Reload credentials if stored/not expired.
        Set appropriate Auth headers on HTTP session.
        """
        raise NotImplementedError

    def request(self, absolute_url, http_method, payload=None, check=None):
        """
        Supports timeout retry, auto re-authentication, accepting DUPLICATE status
        Raises helfpul errors on 4**, 5**, TimeoutErrors, AuthErrors
        """
        raise NotImplementedError

    def extract_data(self, response):
        """Returns dict or list of Python primitive datatypes"""
        raise NotImplementedError


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


class RequestsClient(BasicAuthForTokenMixin, Client):
    CONNECT_TIMEOUT = 3.1
    READ_TIMEOUT = 5
    RETRY_COUNT = 3

    def __init__(self):
        super().__init__()
        self._session = requests.Session()

    def request(self, absolute_url, http_method, payload=None, check=None):
        try:
            response = self._retryable_request(
                absolute_url, http_method, payload, check
            )
        except requests.HTTPError as exc:
            if getattr(exc.response, "status_code", 0) != status.HTTP_401_UNAUTHORIZED:
                raise
        else:
            return response
        self.refresh_auth()
        return self._retryable_request(absolute_url, http_method, payload, check)

    def _retryable_request(self, absolute_url, http_method, payload=None, check=None):
        attempt = 0
        while attempt < self.RETRY_COUNT:
            try:
                response = self._do_request(absolute_url, http_method, payload)
            except requests.Timeout as e:
                attempt += 1
                if attempt == self.RETRY_COUNT:
                    raise requests.Timeout(f"Timed-out {attempt} times.") from e
            else:
                self._check_response(response, check)
                return response

    def _do_request(self, absolute_url, http_method, payload=None):
        return self._session.request(
            http_method,
            url=absolute_url,
            json=payload,
            timeout=(self.CONNECT_TIMEOUT, self.READ_TIMEOUT),
        )

    def _check_response(self, response, check=None):
        if check is None or response.status_code == check:
            return

        if 400 <= response.status_code < 500:
            err_typ = "Client error"
        elif 500 <= response.status_code < 600:
            err_typ = "Server error"
        else:
            err_typ = "Response code"

        if isinstance(response.reason, bytes):
            reason = response.reason.decode("utf-8")
        else:
            reason = response.reason

        try:
            body = response.json()
        except JSONDecodeError:
            body = ""
        else:
            body = "\n" + pformat(body, indent=4)

        raise requests.HTTPError(
            f"{err_typ} {response.status_code}: {reason} (expected {check}) {body}",
            response=response,
        )

    def extract_data(self, response):
        return response.json()


class Resource:
    def __init__(self, client, path):
        self.client = client
        self.collection_path = path

    def list(self, **query_params):
        url = self.client.build_url(self.collection_path, **query_params)
        response = self.client.request(url, "GET", check=status.HTTP_200_OK)
        return self.client.extract_data(response)

    def detail(self, uri, **query_params):
        url = self.client.build_url(f"{self.collection_path}/{uri}", **query_params)
        response = self.client.request(url, "GET", check=status.HTTP_200_OK)
        return self.client.extract_data(response)

    def create(self, **payload):
        url = self.client.build_url(self.collection_path)
        response = self.client.request(
            url, "POST", payload=payload, check=status.HTTP_201_CREATED
        )
        return self.client.extract_data(response)

    def update(self, uri, payload, partial=False, **query_params):
        url = self.client.build_url(f"{self.collection_path}/{uri}", **query_params)
        method = "PATCH" if partial else "PUT"
        response = self.client.request(
            url, method, payload=payload, check=status.HTTP_200_OK
        )
        return self.client.extract_data(response)

    def bulk_create(self, list_payload):
        url = self.client.build_url(self.collection_path)
        response = self.client.request(
            url, "POST", payload=list_payload, check=status.HTTP_201_CREATED
        )
        return self.client.extract_data(response)

    def bulk_update(self, payload, partial=False, **query_params):
        url = self.client.build_url(self.collection_path, **query_params)
        method = "PATCH" if partial else "PUT"
        response = self.client.request(
            url, method, payload=payload, check=status.HTTP_200_OK
        )
        return self.client.extract_data(response)

    def destroy(self, uri, **query_params):
        url = self.client.build_url(f"{self.collection_path}/{uri}", **query_params)
        self.client.request(url, "DELETE", check=status.HTTP_204_NO_CONTENT)
        return

    def bulk_destroy(self, **query_params):
        url = self.client.build_url(self.collection_path, **query_params)
        self.client.request(url, "DELETE", check=status.HTTP_204_NO_CONTENT)
        return


class JobResource(Resource):
    def history(self, uri):
        url = self.client.build_url(f"{self.collection_path}/{uri}/events")
        response = self.client.request(url, "GET", check=status.HTTP_200_OK)
        return self.client.extract_data(response)
