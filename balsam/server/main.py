import logging

from fastapi import FastAPI, HTTPException, Request, WebSocket, status
from fastapi.responses import JSONResponse
from sqlalchemy.orm.exc import NoResultFound

from balsam.server import settings
from balsam.server.utils import TimingMiddleware, setup_logging

from .auth import build_auth_router, user_from_token
from .pubsub import pubsub
from .routers import apps, batch_jobs, events, jobs, sessions, sites, transfers

logger = logging.getLogger("balsam.server.main")

app = FastAPI(
    title="Balsam API",
    version="0.1.0",
)

setup_logging(settings.log_dir, settings.log_level)


@app.exception_handler(NoResultFound)
async def no_result_handler(request: Request, exc: NoResultFound) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={"error": "Not found"},
    )


app.include_router(
    build_auth_router(),
    prefix="/auth",
    tags=["auth"],
    dependencies=[],
)

app.include_router(
    sites.router,
    prefix="/sites",
    tags=["sites"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)

app.include_router(
    apps.router,
    prefix="/apps",
    tags=["apps"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)

app.include_router(
    jobs.router,
    prefix="/jobs",
    tags=["jobs"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)

app.include_router(
    events.router,
    prefix="/events",
    tags=["events"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)

app.include_router(
    batch_jobs.router,
    prefix="/batch-jobs",
    tags=["batch-jobs"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)

app.include_router(
    transfers.router,
    prefix="/transfers",
    tags=["transfers"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)

app.include_router(
    sessions.router,
    prefix="/sessions",
    tags=["sessions"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)


@app.websocket("/subscribe-user")
async def subscribe_user(websocket: WebSocket) -> None:
    """
    Subscribe to a stream of all Job events for the authenticated user.
    """
    # Accept and receive token
    await websocket.accept()
    token = await websocket.receive_text()
    try:
        user = user_from_token(token)
    except HTTPException:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    p = await pubsub.subscribe(user.id)
    while True:
        msg = await p.get_message(timeout=1.0)
        if msg and msg["type"] == "message":
            await websocket.send_bytes(msg["data"])


app.add_middleware(TimingMiddleware, router=app.router)
logger.info("Loaded balsam.server.main")
logger.info(settings.serialize_without_secrets())
