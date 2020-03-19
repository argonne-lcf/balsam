import click
from requests import HTTPError
from pprint import pformat
from json import JSONDecodeError

from .rest_base_client import RESTClient


class DirectAPIClient(RESTClient):
    def __init__(
        self, host, port, username, password, api_root, database="balsam", **kwargs
    ):
        from balsam.util import postgres

        super().__init__(api_root)
        postgres.configure_django_database(
            username=username,
            password=password,
            host=host,
            port=port,
            database=database,
        )
        from rest_framework.test import APIClient as DRFAPIClient

        self._client = DRFAPIClient()
        self.username = username
        self.password = password
        self.refresh_auth()
        self._dispatch = {
            "GET": self._client.get,
            "POST": self._client.post,
            "PUT": self._client.put,
            "PATCH": self._client.patch,
            "DELETE": self._client.delete,
        }

    def refresh_auth(self):
        """
        DirectAPIClient is already making an authenticated connection directly to Postgres.
        Bypass auth by ensuring a User with the same credentials as the database user exists.
        """
        from balsam.server.models import User

        try:
            User.objects.get(username=self.username)
        except User.DoesNotExist:
            User.objects.create_user(
                username=self.username,
                password=self.password,
                email="",
                is_staff=True,
                is_superuser=True,
            )
        self._client.login(username=self.username, password=self.password)

    def interactive_login(self):
        self.refresh_auth()
        click.echo("Established direct connection to Postgres DB")
        return {}

    def request(self, absolute_url, http_method, payload=None):
        client_http_method = self._dispatch[http_method]
        response = client_http_method(path=absolute_url, data=payload, format="json",)
        if response.status_code >= 400:
            self._raise_with_explanation(response)
        return response

    def _raise_with_explanation(self, response):
        """
        Add the API's informative error message to Requests' generic status Exception
        """
        try:
            explanation = getattr(response, "data")
        except (JSONDecodeError, AttributeError):
            explanation = ""
        else:
            explanation = "\n" + pformat(explanation, indent=4)

        if 400 <= response.status_code < 500:
            raise HTTPError(f"Client error {response.status_code}{explanation}")
        elif 500 <= response.status_code:
            raise HTTPError(f"Server error {response.status_code}{explanation}")

    def extract_data(self, response):
        """
        Return response payload
        """
        return response.data
