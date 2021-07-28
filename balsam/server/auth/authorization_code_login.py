import logging
import secrets
from typing import Any, Dict, Optional
from urllib.parse import urlencode

import requests
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session, exc

from balsam.schemas import UserOut
from balsam.server import settings
from balsam.server.models import get_session
from balsam.server.models.crud import users

logger = logging.getLogger(__name__)


router = APIRouter(prefix="/oauth")


def generate_state(user_code: Optional[str] = None) -> str:
    """
    Produce new random state to store in DB. If Device Code flow, we have a
    user_code and the state is associated with a user_code.
    """
    secret = secrets.token_urlsafe()
    if user_code is None:
        return f"BROWSER-FLOW {secret}"
    return f"DEVICE-FLOW {user_code} {secret}"


def state_to_usercode(state: str) -> Optional[str]:
    """
    Extract user_code from DB state.  Returns None
    if this is not part of a Device Code flow.
    """
    if state.startswith("DEVICE-FLOW"):
        return state.split(" ")[1]
    return None


def alcf_username_from_token(token: str) -> str:
    """
    Trade ALCF-OAuth access token for user info.
    Returns the username for Balsam to get a unique identity.
    """
    conf = settings.auth.oauth_provider
    assert conf is not None
    resp = requests.get(
        conf.user_info_uri,
        headers={"Authorization": f"Bearer {token}"},
    )
    logger.debug(f"user info response: {resp.status_code}\n{resp.text}")
    dat = resp.json()
    logger.debug(f"user info response json: {dat}")
    return str(dat["username"])


def redirect_to_oauth_provider(request: Request, state: str) -> RedirectResponse:
    """
    Direct the user agent to the OAuth provider to authorize.
    Used by both pure-Authorization code and hybrid Device Code flows.
    """
    conf = settings.auth.oauth_provider
    assert conf is not None, "Using OAuth without configuration"
    callback_uri = f"{conf.redirect_scheme}://{request.url.netloc}" + conf.redirect_path
    params = urlencode(
        dict(
            response_type="code",
            client_id=conf.client_id,
            redirect_uri=callback_uri,
            scope=conf.scope,
            state=state,
        )
    )
    return RedirectResponse(conf.request_uri + "?" + params)


@router.get("/ALCF/login/device")
def start_login_device(request: Request, user_code: str, db: Session = Depends(get_session)) -> RedirectResponse:
    """
    User provides user code to proceed with "device code flow" (started by a CLI login client)
    Balsam hands off to ALCF OAuth for authenticating user via "authorization code flow"
    """
    try:
        device_code_attempt = users.get_device_code_attempt_by_user(db, user_code)
    except exc.NoResultFound:
        raise HTTPException(status_code=400, detail="invalid_client")
    state = generate_state(user_code=device_code_attempt.user_code)
    users.add_auth_state(db, state)
    db.commit()
    return redirect_to_oauth_provider(request, state)


@router.get("/ALCF/login")
def start_login(request: Request, db: Session = Depends(get_session)) -> RedirectResponse:
    """
    User initiated Web browser authorization code flow
    """
    state = generate_state(user_code=None)
    users.add_auth_state(db, state)
    db.commit()
    return redirect_to_oauth_provider(request, state)


@router.get("/ALCF/callback")
def callback(
    request: Request,
    code: str,
    state: str,
    error: Optional[str] = None,
    error_description: Optional[str] = None,
    db: Session = Depends(get_session),
) -> Dict[str, Any]:
    """
    https://tools.ietf.org/html/rfc6749#section-4.1.2.1
    Callback from OAuth provider.  This is used by both pure-Authorization code
    and hybrid Device code flows.
    """
    # If user_code is not None, we're on the second half of a device-code login flow
    user_code = state_to_usercode(state)

    if error == "access_denied" and user_code is not None:
        users.deny_device_code_attempt(db, user_code)
        db.commit()

    if error:
        logger.warning(f"OAuth error response: {error}")
        if error_description:
            logger.warning(f"OAuth error description: {error_description}")
        return {"error": error, "error_description": error_description}

    # Ensure the "state" is in our DB to mitigate CSRF
    try:
        users.verify_auth_state(db, state)
    except exc.NoResultFound:
        raise HTTPException(status_code=400, detail="invalid_state")

    # Use `code` to request an access token from the OAuth provider
    conf = settings.auth.oauth_provider
    assert conf is not None, "Using OAuth without configuration"
    callback_uri = f"{conf.redirect_scheme}://{request.url.netloc}" + conf.redirect_path
    token_params = dict(
        client_id=conf.client_id,
        client_secret=conf.client_secret,
        code=code,
        redirect_uri=callback_uri,
        grant_type="authorization_code",
    )
    logger.debug(f"Requesting token from {conf.token_uri}: {token_params}")
    token_response = requests.post(
        conf.token_uri,
        data=token_params,
        headers={"Cache-Control": "no-cache"},
    )
    logger.debug(f"Received token response: {token_response.status_code}")
    token_data = token_response.json()
    logger.debug(f"token response json: {token_data}")
    access_token = str(token_data["access_token"])

    # Use access token to get ALCF username
    username = alcf_username_from_token(access_token)
    try:
        from_db = users.get_user_by_username(db, username)
        user = UserOut(id=from_db.id, username=from_db.username)
        logger.info(f"Looked up existing username: {username}")
    except exc.NoResultFound:
        user = users.create_user(db, username, password=None)
        db.commit()
        logger.info(f"Created new Balsam username: {username}")

    # If this is coupled to a Device Code login flow, authorize the device code
    # attempt
    if user_code:
        logger.info(f"Authorizing device code attempt for user code: {user_code}")
        users.authorize_device_code_attempt(db, user_code, user)
        db.commit()
    else:
        logger.debug("No user code; this is an ordinary authorization code callback.")
    return {"username": username}
