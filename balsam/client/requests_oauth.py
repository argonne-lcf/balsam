import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, Optional
from uuid import UUID, uuid4

import click
import dateutil
from requests import HTTPError

from balsam.util import Spinner

from . import urls
from .requests_client import RequestsClient
from .rest_base_client import AuthError

logger = logging.getLogger(__name__)


class OAuthRequestsClient(RequestsClient):
    def __init__(
        self,
        api_root: str,
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
        self.token = token
        self.token_expiry = token_expiry
        self.expires_in = timedelta(hours=240)
        self.login_attempt_id: Optional[UUID] = None
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
        self.session.auth = None
        self.session.headers["Authorization"] = f"Bearer {self.token}"
        self._authenticated = True

    def prompt_user(self, auth_uri_complete: str) -> None:
        click.echo(f"Logging into Balsam API at {self.api_root}")
        click.echo(f"To proceed, please navigate to: {auth_uri_complete}")
        click.echo("Authenticate with your credentials then come back here!")

    def poll_for_token(self, device_code: str, user_code: str, expires_in: float, interval: float) -> None:
        data = {
            "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
            "device_code": device_code,
            "client_id": self.login_attempt_id,
        }
        num_attempts = int(expires_in / interval)
        for _ in range(num_attempts):
            time.sleep(interval)
            try:
                resp = self.request(urls.DEVICE_TOKEN, "POST", data=data, authenticating=True)
                break
            except HTTPError as exc:
                if exc.response.status_code == 400 and "authorization_pending" in exc.response.text:
                    continue
                raise
        else:
            raise AuthError(f"Login attempt timed out after {expires_in} seconds. Please try again.")

        if isinstance(resp, dict) and "access_token" in resp:
            self.token = resp["access_token"]
            self.token_expiry = dateutil.parser.parse(resp["expiration"])
        else:
            raise AuthError(f"Could not authenticate: {resp}")

    def interactive_login(self) -> Dict[str, Any]:
        """Initiate interactive login flow"""
        self.login_attempt_id = uuid4()
        resp = self.request(urls.DEVICE_LOGIN, "POST", data={"client_id": self.login_attempt_id}, authenticating=True)
        assert isinstance(resp, dict)
        if not (resp.get("device_code") and resp.get("user_code")):
            raise AuthError(f"Did not get expected device authorization response: {resp}")

        resp.pop("verification_uri")
        auth_uri_comp = resp.pop("verification_uri_complete")
        self.prompt_user(auth_uri_comp)
        with Spinner("Waiting for user log in..."):
            self.poll_for_token(**resp)
        self.refresh_auth()
        click.echo("Logged In")
        return {"token": self.token, "token_expiry": self.token_expiry}


RequestsClient._client_classes["oauth_device"] = OAuthRequestsClient
