import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import click
import dateutil.parser

from . import urls
from .requests_client import RequestsClient
from .rest_base_client import AuthError

logger = logging.getLogger(__name__)


class BasicAuthRequestsClient(RequestsClient):
    def __init__(
        self,
        api_root: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None,
        token_expiry: Optional[datetime] = None,
        connect_timeout: float = 3.1,
        read_timeout: float = 120.0,
        retry_count: int = 3,
    ) -> None:
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
        self.expires_in = timedelta(hours=240)
        if self.token is not None:
            self.session.auth = None
            self.session.headers["Authorization"] = f"Bearer {self.token}"
            self._authenticated = True
        if self.token_expiry:
            self.expires_in = self.token_expiry - datetime.utcnow()
            if self.expires_in.total_seconds() <= 1:
                self._authenticated = False
                logger.warning("Auth Token is expired; please refresh with `balsam login`")
            elif self.expires_in < timedelta(hours=24):
                logger.warning(f"Auth Token will expire in {self.expires_in}; please refresh with `balsam login`")

    def refresh_auth(self) -> None:
        if self.username is None or self.password is None:
            raise ValueError("Cannot refresh_auth without username and password. Please provide these values.")
        # Login with HTTPBasic Auth to get a token:
        cred = {"username": self.username, "password": self.password}
        resp = self.request(urls.PASSWORD_LOGIN, "POST", data=cred, authenticating=True)
        assert isinstance(resp, dict)
        if resp is not None and "access_token" in resp:
            self.token = resp["access_token"]
            self.token_expiry = dateutil.parser.parse(resp["expiration"])
        else:
            raise AuthError(f"Could not authenticate: {resp}")

        # Unset BasicAuth; set Token Authorization header
        self.session.auth = None
        self.session.headers["Authorization"] = f"Bearer {self.token}"
        self._authenticated = True

    def interactive_login(self) -> Dict[str, Any]:
        """Initiate interactive login flow"""
        self.username = click.prompt("Balsam username", hide_input=False)
        self.password = click.prompt("Balsam password", hide_input=True)
        self.refresh_auth()
        click.echo("Logged In")
        return {"token": self.token, "token_expiry": self.token_expiry}


RequestsClient._client_classes["password"] = BasicAuthRequestsClient
