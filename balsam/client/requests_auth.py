import click
import dateutil.parser
from .requests_client import RequestsClient
from .rest_base_client import AuthError
import time
from requests.exceptions import ConnectionError


class BasicAuthRequestsClient(RequestsClient):
    def __init__(
        self,
        api_root,
        username,
        password=None,
        token=None,
        token_expiry=None,
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
        self.token = token
        self.token_expiry = token_expiry

    def refresh_auth(self):
        if self.password is None:
            raise ValueError(
                "Cannot refresh_auth: self.password is None. Please provide a password"
            )
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
            self.token = resp["access_token"]
            self.token_expiry = dateutil.parser.parse(resp["expiration"])
        except KeyError:
            raise AuthError(f"Could not authenticate: {resp}")

        # Unset BasicAuth; set Token Authorization header
        self._session.auth = None
        self._session.headers["Authorization"] = f"Bearer {self.token}"

    def interactive_login(self):
        """Initiate interactive login flow"""
        self.password = click.prompt("Balsam password", hide_input=True)
        self.refresh_auth()
        click.echo("Logged In")
        return {"token": self.token, "token_expiry": self.token_expiry}
