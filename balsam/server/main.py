import sys
from typing import Any
import logging
import logging.handlers

from fastapi import FastAPI, HTTPException, Request, WebSocket, status
from fastapi.responses import JSONResponse
from sqlalchemy.orm.exc import NoResultFound

from balsam.server import settings

from .auth import user_from_token
from .auth.router import auth_router
from .pubsub import pubsub
from .routers import apps, batch_jobs, events, jobs, sessions, sites, transfers

app = FastAPI(
    title="Balsam API",
    version="0.1.0",
)


def log_uncaught_exceptions(exctype: Any, value: Any, tb: Any) -> None:
    root_logger = logging.getLogger("balsam.server")
    root_logger.error(f"Uncaught Exception {exctype}: {value}", exc_info=(exctype, value, tb))


def setup_logging() -> logging.Logger:
    logging.getLogger("balsam").handlers.clear()

    logger = logging.getLogger("balsam.server")
    logger.handlers.clear()
    format = "%(asctime)s.%(msecs)03d | %(process)d | %(levelname)s | %(name)s:%(lineno)s] %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(format, datefmt=datefmt)

    handler = logging.handlers.RotatingFileHandler(
        filename=settings.log_dir / "server-balsam.log",
        maxBytes=int(32 * 1e6),
        backupCount=3,
    ) if settings.log_dir else logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.setLevel(settings.log_level)
    logger.addHandler(handler)
    sys.excepthook = log_uncaught_exceptions
    return logging.getLogger("balsam.server.main")


logger = setup_logging()


@app.exception_handler(NoResultFound)
async def no_result_handler(request: Request, exc: NoResultFound) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={"error": "Not found"},
    )


app.include_router(
    auth_router,
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


logger.info(f"Loaded balsam.server.main\n{settings}")
