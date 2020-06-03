import click
from .requests_client import RequestsClient
from .rest_base_client import AuthError


class BasicAuthRequestsClient(RequestsClient):
    def __init__(
        self,
        host,
        port,
        username,
        password,
        scheme,
        api_root,
        connect_timeout=3.1,
        read_timeout=5,
        retry_count=3,
        **kwargs,
    ):
        url = f'{scheme}://{host}:{port}/{api_root.strip("/")}'
        super().__init__(
            url,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            retry_count=retry_count,
        )
        self.username = username
        self.password = password

    def refresh_auth(self):
        # Login with HTTPBasic Auth to get a token:
        self._session.auth = (self.username, self.password)
        login_url = self.build_url("login")
        resp = self._session.post(login_url)
        try:
            token = resp.json()["token"]
        except KeyError:
            raise AuthError(f"Could not authenticate: {resp}")

        # Unset BasicAuth; set Token Authorization header
        self._session.auth = None
        self._session.headers["Authorization"] = f"Token {token}"

    def interactive_login(self):
        """Initiate interactive login flow"""
        self.refresh_auth()
        click.echo("Logged In")
