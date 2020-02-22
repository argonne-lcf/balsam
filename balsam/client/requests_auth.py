from .requests_client import RequestsClient


class BasicAuthRequestsClient(RequestsClient):
    def __init__(
        self,
        api_root,
        username,
        password,
        connect_timeout=3.1,
        read_timeout=5,
        retry_count=3,
    ):
        super().__init__(api_root, connect_timeout=3.1, read_timeout=5, retry_count=3)
        self.username = username
        self.password = password

    def refresh_auth(self):
        # Login with HTTPBasic Auth to get a token:
        self._session.auth = (self.username, self.password)
        login_url = self.build_url("login")
        resp = self._session.post(login_url)
        token = resp.json()["token"]

        # Unset BasicAuth; set Token Authorization header
        self._session.auth = None
        self._session.headers["Authorization"] = f"Token {token}"

    def interactive_login(self):
        """Initiate interactive login flow"""
        raise NotImplementedError
