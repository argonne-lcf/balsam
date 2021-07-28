import logging
import secrets
import string
from datetime import datetime
from typing import Any, Dict
from uuid import UUID

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from sqlalchemy.orm import Session, exc

from balsam.schemas import UserOut
from balsam.server import settings
from balsam.server.models import get_session
from balsam.server.models.crud import users

from .token import create_access_token

logger = logging.getLogger(__name__)

VERIFICATION_PATH = "/auth/oauth/ALCF/login/device"
GRANT_TYPE = "urn:ietf:params:oauth:grant-type:device_code"

router = APIRouter(prefix="/device")


def generate_device_code() -> str:
    """
    Returns a high-entropy device code (never typed or copy-pasted)
    """
    return secrets.token_urlsafe(nbytes=256)


def generate_user_code() -> str:
    """
    Returns an easy-to-type code like "WDJB-MJHT" with 20^8 entropy.
    Following the guidelines for user codes suggested here:
    https://tools.ietf.org/html/rfc8628#section-6.1
    """
    consonants = list(set(string.ascii_uppercase) - set("AEIOUY"))
    part1 = "".join(secrets.choice(consonants) for _ in range(4))
    part2 = "".join(secrets.choice(consonants) for _ in range(4))
    return f"{part1}-{part2}"


@router.post("/login")
def authorization_request(
    request: Request,
    db: Session = Depends(get_session),
    client_id: UUID = Form(...),
    scope: str = Form(""),
) -> Dict[str, Any]:
    """
    The device client initiates the authorization flow
    https://tools.ietf.org/html/rfc8628
    """
    logger.debug(f"Creating device code for device client login attempt {client_id}")
    conf = settings.auth.oauth_provider
    assert conf is not None
    device_code = generate_device_code()
    user_code = generate_user_code()
    verification_uri = f"{conf.redirect_scheme}://{request.url.netloc}" + VERIFICATION_PATH
    verification_uri_complete = verification_uri + f"?user_code={user_code}"
    expires_in = conf.device_code_lifetime.total_seconds()
    interval = conf.device_poll_interval.total_seconds()

    response = {
        "device_code": device_code,
        "user_code": user_code,
        "verification_uri": verification_uri,
        "verification_uri_complete": verification_uri_complete,
        "expires_in": expires_in,
        "interval": interval,
    }
    expiration_date = datetime.utcnow() + conf.device_code_lifetime
    users.create_device_code_attempt(
        db=db,
        client_id=client_id,
        expiration=expiration_date,
        device_code=device_code,
        user_code=user_code,
        scope=scope,
    )
    logger.debug(f"Device login flow with user-code {user_code} will expire at {expiration_date}")
    users.cleanup_device_code_attempts(db)
    db.commit()

    return response


@router.post("/token")
def access_token_request(
    db: Session = Depends(get_session),
    grant_type: str = Form(...),
    device_code: str = Form(...),
    client_id: UUID = Form(...),
) -> Dict[str, Any]:
    """
    The device client polls this endpoint until the user has approved the grant.
    https://tools.ietf.org/html/rfc8628#section-3.4
    """
    if grant_type != GRANT_TYPE:
        raise HTTPException(status_code=400, detail="unsupported_grant_type")

    try:
        logger.debug(f"Look up device_code attempt for device client {client_id}")
        device_code_attempt = users.get_device_code_attempt_by_client(db, client_id, device_code)
    except exc.NoResultFound:
        # Can't find that device code
        raise HTTPException(status_code=400, detail="invalid_client")

    if device_code_attempt.user_denied:
        # The user decided NOT to give Balsam their identity
        raise HTTPException(status_code=403, detail="access_denied")

    if datetime.utcnow() >= device_code_attempt.expiration:
        # This Device code login attempt is too old
        raise HTTPException(status_code=400, detail="expired_token")

    if device_code_attempt.user_id is None:
        # We haven't associated this device code with an ALCF user yet.
        # Client should wait a few seconds and try again.
        logger.debug(f"authorization pending for device code attempt {client_id}")
        raise HTTPException(status_code=400, detail="authorization_pending")

    user = UserOut(id=device_code_attempt.user.id, username=device_code_attempt.user.username)
    logger.debug(f"device code authorization success! associating {client_id} with username {user.username}")
    token, expiry = create_access_token(user)
    users.delete_device_code_attempt(db, device_code)
    db.commit()
    return {"access_token": token, "token_type": "bearer", "expiration": expiry}
