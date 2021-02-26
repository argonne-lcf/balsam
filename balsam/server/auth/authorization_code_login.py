import secrets
from typing import Any, Dict, Optional
from urllib.parse import urlencode

import requests
from fastapi import Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session, exc

from balsam.schemas import UserOut
from balsam.server import settings
from balsam.server.auth import auth_router
from balsam.server.models import get_session
from balsam.server.models.crud import users


def generate_state(user_code: Optional[str] = None) -> str:
    secret = secrets.token_urlsafe()
    if user_code is None:
        return f"BROWSER-FLOW {secret}"
    return f"DEVICE-FLOW {user_code} {secret}"


def state_to_usercode(state: str) -> Optional[str]:
    if state.startswith("DEVICE-FLOW"):
        return state.split(" ")[1]
    return None


def alcf_username_from_token(token: str) -> str:
    # TODO: Get user data from an ALCF endpoint
    return "TEST-USER"


def redirect_to_oauth_provider(request: Request, state: str) -> RedirectResponse:
    conf = settings.auth.oauth_provider
    assert conf is not None, "Using OAuth without configuration"
    callback_uri = f"{request.url.scheme}://{request.url.netloc}" + conf.redirect_path
    params = urlencode(
        dict(
            client_id=conf.client_id,
            scope=conf.scope,
            response_type="code",
            redirect_uri=callback_uri,
            state=state,
        )
    )
    return RedirectResponse(conf.request_uri + "?" + params)


@auth_router.get("/ALCF/login/device")
def start_login_device(request: Request, user_code: str, db: Session = Depends(get_session)) -> RedirectResponse:
    """
    User provides user code to proceed with "device code flow" (from CLI login client)
    Balsam hands off to ALCF OAuth for authenticating user via "authorization code flow"
    """
    try:
        device_code_attempt = users.get_device_code_attempt_by_user(db, user_code)
    except exc.NoResultFound:
        raise HTTPException(status_code=400, detail="invalid_client")
    state = generate_state(user_code=device_code_attempt.user_code)
    users.add_auth_state(db, state)
    return redirect_to_oauth_provider(request, state)


@auth_router.get("/ALCF/login")
def start_login(request: Request, db: Session = Depends(get_session)) -> RedirectResponse:
    """
    User initiated Web browser authorization code flow
    """
    state = generate_state(user_code=None)
    users.add_auth_state(db, state)
    return redirect_to_oauth_provider(request, state)


@auth_router.get("/ALCF/callback")
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
    """
    # If user_code is not None, we're on the second half of a device-code login flow
    user_code = state_to_usercode(state)

    if error == "access_denied" and user_code is not None:
        users.deny_device_code_attempt(db, user_code)

    if error:
        print("OAuth error response:", error)
        if error_description:
            print("Error description:", error_description)
        return {"error": error, "error_description": error_description}

    try:
        users.verify_auth_state(db, state)
    except exc.NoResultFound:
        raise HTTPException(status_code=400, detail="invalid_state")

    conf = settings.auth.oauth_provider
    assert conf is not None, "Using OAuth without configuration"
    callback_uri = f"{request.url.scheme}://{request.url.netloc}" + conf.redirect_path
    token_params = dict(
        grant_type="authorization_code",
        code=code,
        redirect_uri=callback_uri,
        client_id=conf.client_id,
    )
    token_response = requests.post(
        conf.token_uri,
        headers={"Authorization": f"Basic {conf.client_secret}"},
        params=token_params,
    )
    print("Received token_response:", token_response.status_code)
    token_data = token_response.json()
    print(token_data)

    username = alcf_username_from_token(token_data)
    try:
        from_db = users.get_user_by_username(db, username)
        user = UserOut(id=from_db.id, username=from_db.username)
        print("Looked up existing username", username)
    except exc.NoResultFound:
        user = users.create_user(db, username, password=None)
        print("Created new username", username)

    if user_code:
        users.authorize_device_code_attempt(db, user_code, user)
        print("Authorized device code attempt for user code:", user_code)
    return {"username": username}
