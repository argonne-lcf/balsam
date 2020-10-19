import click
from .requests_client import RequestsClient
from .rest_base_client import AuthError
import time
from requests.exceptions import ConnectionError


class BasicAuthRequestsClient(RequestsClient):
    def __init__(
        self,
        api_root,
        username,
        password,
        connect_timeout=3.1,
        read_timeout=60,
        retry_count=3,
    ):
        super().__init__(
            api_root,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            retry_count=retry_count,
        )
        self.username = username
        self.password = password

    def refresh_auth(self):
        # Login with HTTPBasic Auth to get a token:
        url = "users/login"
        cred = {"username": self.username, "password": self.password}
        resp = None
        for _ in range(3):
            try:
                resp = self.request(url, "POST", data=cred, refresh_auth=False)
            except ConnectionError:
                time.sleep(1)
            else:
                break
        try:
            token = resp["access_token"]
        except KeyError:
            raise AuthError(f"Could not authenticate: {resp}")

        # Unset BasicAuth; set Token Authorization header
        self._session.auth = None
        self._session.headers["Authorization"] = f"Bearer {token}"

    def interactive_login(self):
        """Initiate interactive login flow"""
        self.refresh_auth()
        click.echo("Logged In")
