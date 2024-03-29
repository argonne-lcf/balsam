import os
import random
import time
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar, Union

import click
import globus_sdk
from configobj import ConfigObj  # type: ignore
from globus_sdk import (
    ConfidentialAppAuthClient,
    GlobusHTTPResponse,
    NativeAppAuthClient,
    OAuthTokenResponse,
    RefreshTokenAuthorizer,
    TransferClient,
)
from globus_sdk.exc import GlobusAPIError, NetworkError

EitherAuthClient = Union[ConfidentialAppAuthClient, NativeAppAuthClient]

CLIENT_ID_OPTNAME = "client_id"
CLIENT_SECRET_OPTNAME = "client_secret"
TEMPLATE_ID_OPTNAME = "template_id"
DEFAULT_TEMPLATE_ID = "b1ca1cb7-ae70-451c-be61-4b2a61a02e9c"
TRANSFER_AT_EXPIRES_OPTNAME = "transfer_access_token_expires"
TRANSFER_RT_OPTNAME = "transfer_refresh_token"
TRANSFER_AT_OPTNAME = "transfer_access_token"
AUTH_RT_OPTNAME = "auth_refresh_token"
AUTH_AT_OPTNAME = "auth_access_token"
AUTH_AT_EXPIRES_OPTNAME = "auth_access_token_expires"
SCOPES = (
    "openid profile email "
    "urn:globus:auth:scope:auth.globus.org:view_identity_set "
    "urn:globus:auth:scope:transfer.api.globus.org:all "
)

T = TypeVar("T")


class RetryingTransferClient(TransferClient):
    """
    Wrapper around TransferClient that retries safe resources on NetworkErrors
    """

    default_retries = 10

    def __init__(self, tries: Optional[int] = None, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.tries = tries or self.default_retries

    def retry(self, f: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """
        Retries the given function self.tries times on NetworkErros
        """
        backoff = random.random() / 100  # 5ms on average
        for _ in range(self.tries - 1):
            try:
                return f(*args, **kwargs)
            except NetworkError:
                time.sleep(backoff)
                backoff *= 2
        return f(*args, **kwargs)

    # get and put should always be safe to retry
    def get(self, *args: Any, **kwargs: Any) -> GlobusHTTPResponse:
        return self.retry(super().get, *args, **kwargs)

    def put(self, *args: Any, **kwargs: Any) -> GlobusHTTPResponse:
        return self.retry(super().put, *args, **kwargs)

    # task submission is safe, as the data contains a unique submission-id
    def submit_transfer(self, *args: Any, **kwargs: Any) -> GlobusHTTPResponse:
        return self.retry(super().submit_transfer, *args, **kwargs)

    def submit_delete(self, *args: Any, **kwargs: Any) -> GlobusHTTPResponse:
        return self.retry(super().submit_delete, *args, **kwargs)


def _update_tokens(token_response: OAuthTokenResponse) -> None:
    tokens = token_response.by_resource_server["transfer.api.globus.org"]
    set_transfer_tokens(tokens["access_token"], tokens["refresh_token"], tokens["expires_at_seconds"])


def set_transfer_tokens(access_token: str, refresh_token: str, expires_at: str) -> None:
    write_option(TRANSFER_AT_OPTNAME, access_token)
    write_option(TRANSFER_RT_OPTNAME, refresh_token)
    write_option(TRANSFER_AT_EXPIRES_OPTNAME, expires_at)


def get_config_obj(system: bool = False, file_error: bool = False) -> ConfigObj:
    if system:
        path = "/etc/globus.cfg"
    else:
        path = os.path.expanduser("~/.balsam/globus.cfg")

    conf = ConfigObj(path, encoding="utf-8", file_error=file_error)

    # delete any old whomai values in the cli section
    for key in conf.get("cli", {}):
        if "whoami_identity_" in key:
            del conf["cli"][key]
            conf.write()

    return conf


def lookup_option(option: str, section: str = "cli", environment: Optional[str] = None) -> Any:
    conf = get_config_obj()
    try:
        if environment:
            return conf["environment " + environment][option]
        else:
            return conf[section][option]
    except KeyError:
        return None


def write_option(option: str, value: Any, section: str = "cli", system: bool = False) -> None:
    """
    Write an option to disk -- doesn't handle config reloading
    """
    # deny rwx to Group and World -- don't bother storing the returned old mask
    # value, since we'll never restore it in the CLI anyway
    # do this on every call to ensure that we're always consistent about it
    os.umask(0o077)

    # FIXME: DRY violation with config_commands.helpers
    conf = get_config_obj(system=system)

    # add the section if absent
    if section not in conf:
        conf[section] = {}

    conf[section][option] = value
    conf.write()


def get_transfer_tokens() -> Dict[str, Any]:
    expires = lookup_option(TRANSFER_AT_EXPIRES_OPTNAME)
    if expires is not None:
        expires = int(expires)

    return {
        "refresh_token": lookup_option(TRANSFER_RT_OPTNAME),
        "access_token": lookup_option(TRANSFER_AT_OPTNAME),
        "access_token_expires": expires,
    }


def internal_native_client() -> NativeAppAuthClient:
    template_id = lookup_option(TEMPLATE_ID_OPTNAME) or DEFAULT_TEMPLATE_ID
    return NativeAppAuthClient(template_id)


def internal_auth_client(requires_instance: bool = False, force_new_client: bool = False) -> EitherAuthClient:
    """
    Looks up the values for this CLI's Instance Client in config

    If none exists and requires_instance is True or force_new_client is True,
    registers a new Instance Client with Globus Auth

    If none exists and requires_instance is false, defaults to a Native Client
    for backwards compatibility

    Returns either a NativeAppAuthClient or a ConfidentialAppAuthClient
    """
    client_id = lookup_option(CLIENT_ID_OPTNAME)
    client_secret = lookup_option(CLIENT_SECRET_OPTNAME)
    template_id = lookup_option(TEMPLATE_ID_OPTNAME) or DEFAULT_TEMPLATE_ID
    template_client = internal_native_client()
    existing = client_id and client_secret

    # if we are forcing a new client, delete any existing client
    if force_new_client and existing:
        existing_client = ConfidentialAppAuthClient(client_id, client_secret)
        try:
            existing_client.delete(f"/v2/api/clients/{client_id}")

        # if the client secret has been invalidated or the client has
        # already been removed, we continue on
        except globus_sdk.exc.GlobusAPIError:
            pass

    # if we require a new client to be made
    if force_new_client or (requires_instance and not existing):
        # register a new instance client with auth
        body = {"client": {"template_id": template_id, "name": "Balsam"}}
        res = template_client.post("/v2/api/clients", data=body)

        # get values and write to config
        credential_data = res["included"]["client_credential"]
        client_id = credential_data["client"]
        client_secret = credential_data["secret"]
        write_option(CLIENT_ID_OPTNAME, client_id)
        write_option(CLIENT_SECRET_OPTNAME, client_secret)

        return ConfidentialAppAuthClient(client_id, client_secret, app_name="Balsam")

    # if we already have a client, just return it
    elif existing:
        return ConfidentialAppAuthClient(client_id, client_secret, app_name="Balsam")

    # fall-back to a native client to not break old logins
    # TOOD: eventually remove this behavior
    else:
        return template_client


def get_client() -> RetryingTransferClient:
    tokens = get_transfer_tokens()
    authorizer = None

    # if there's a refresh token, use it to build the authorizer
    if tokens["refresh_token"] is not None:
        authorizer = RefreshTokenAuthorizer(
            tokens["refresh_token"],
            internal_auth_client(),
            access_token=tokens["access_token"],
            expires_at=tokens["access_token_expires"],
            on_refresh=_update_tokens,
        )

    return RetryingTransferClient(authorizer=authorizer, app_name="Balsam")


def exchange_code_and_store_config(auth_client: EitherAuthClient, auth_code: str) -> None:
    """
    Finishes auth flow after code is gotten from command line or local server.
    Exchanges code for tokens and gets user info from auth.
    Stores tokens and user info in config.
    """
    # do a token exchange with the given code
    tkn_response = auth_client.oauth2_exchange_code_for_tokens(auth_code)
    tkn = tkn_response.by_resource_server

    store_queue: List[Tuple[str, str, bool]] = []

    def _enqueue(optname: str, newval: str, revoke: bool = True) -> None:
        store_queue.append((optname, newval, revoke))

    # extract access tokens from final response
    if "transfer.api.globus.org" in tkn:
        _enqueue(TRANSFER_RT_OPTNAME, tkn["transfer.api.globus.org"]["refresh_token"])
        _enqueue(TRANSFER_AT_OPTNAME, tkn["transfer.api.globus.org"]["access_token"])
        _enqueue(
            TRANSFER_AT_EXPIRES_OPTNAME,
            tkn["transfer.api.globus.org"]["expires_at_seconds"],
            revoke=False,
        )
    if "auth.globus.org" in tkn:
        _enqueue(AUTH_RT_OPTNAME, tkn["auth.globus.org"]["refresh_token"])
        _enqueue(AUTH_AT_OPTNAME, tkn["auth.globus.org"]["access_token"])
        _enqueue(
            AUTH_AT_EXPIRES_OPTNAME,
            tkn["auth.globus.org"]["expires_at_seconds"],
            revoke=False,
        )

    # revoke any existing tokens
    for optname in [optname for (optname, _val, revoke) in store_queue if revoke]:
        token = lookup_option(optname)
        if token:
            auth_client.oauth2_revoke_token(token)

    # write new data to config
    for optname, newval, _revoke in store_queue:
        write_option(optname, newval)


def do_link_auth_flow(
    session_params: Optional[Dict[str, Any]] = None, force_new_client: bool = False, endpoint_ids: str = ""
) -> bool:
    """
    Prompts the user with a link to authenticate with globus auth
    and authorize the CLI to act on their behalf.
    """
    session_params = session_params or {}

    # get the ConfidentialApp client object
    auth_client = internal_auth_client(requires_instance=True, force_new_client=force_new_client)

    # add optional endpoint to default scopes
    scopes = SCOPES
    if endpoint_ids:
        for e in endpoint_ids:
            scopes += (
                f"urn:globus:auth:scope:transfer.api.globus.org:all[*https://auth.globus.org/scopes/{e}/data_access] "
            )

    # start the Confidential App Grant flow
    auth_client.oauth2_start_flow(
        redirect_uri=auth_client.base_url + "v2/web/auth-code",
        refresh_tokens=True,
        requested_scopes=scopes,
    )

    # prompt
    additional_params = {"prompt": "login"}
    additional_params.update(session_params)
    linkprompt = "Please authenticate with Globus here"
    click.echo(
        "{0}:\n{1}\n{2}\n{1}\n".format(
            linkprompt,
            "-" * len(linkprompt),
            auth_client.oauth2_get_authorize_url(),
        )
    )

    # come back with auth code
    auth_code = click.prompt("Enter the resulting Authorization Code here").strip()

    # finish auth flow
    exchange_code_and_store_config(auth_client, auth_code)
    return True


def check_logged_in() -> bool:
    # first, try to get the refresh tokens from config
    # we can skip the access tokens and their expiration times as those are not
    # strictly necessary
    transfer_rt = lookup_option(TRANSFER_RT_OPTNAME)
    auth_rt = lookup_option(AUTH_RT_OPTNAME)

    # if either of the refresh tokens are null return False
    if transfer_rt is None or auth_rt is None:
        return False

    # get or create the instance client
    auth_client = internal_auth_client(requires_instance=True)

    # check that tokens and client are valid
    try:
        for tok in (transfer_rt, auth_rt):
            res = auth_client.oauth2_validate_token(tok)
            if not res["active"]:
                return False

    # if the instance client is invalid, an GlobusAPIError will be raised
    # we then force a new client to be created before continuing
    except GlobusAPIError:
        return False

    return True
