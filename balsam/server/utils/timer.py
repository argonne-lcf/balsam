import logging
import resource
import time
from typing import Awaitable, Callable

from fastapi import APIRouter, FastAPI, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.routing import Match, Mount

logger = logging.getLogger(__name__)


def _get_cpu_time() -> float:
    """
    Generates the cpu time to report. Adds the user and system time, following the implementation from timing-asgi
    """
    resources = resource.getrusage(resource.RUSAGE_SELF)
    # add up user time (ru_utime) and system time (ru_stime)
    return resources[0] + resources[1]


class TimingMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: FastAPI, router: APIRouter):
        super().__init__(app)
        self.router = router

    def get_route_name(self, request: Request) -> str:
        scope = request.scope
        route = None
        for r in self.router.routes:
            if r.matches(scope)[0] == Match.FULL:
                route = r
                break
        if hasattr(route, "endpoint") and hasattr(route, "name"):
            name = f"{route.endpoint.__module__}.{route.name}"  # type: ignore
        elif isinstance(route, Mount):
            name = f"{type(route.app).__name__}<{route.name!r}>"
        else:
            name = str(f"<Path: {scope['path']}>")
        return name

    async def dispatch(self, request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        start_wall, start_cpu = time.perf_counter(), _get_cpu_time()
        response = await call_next(request)
        wall, cpu = time.perf_counter() - start_wall, _get_cpu_time() - start_cpu
        timing_info = f"TIMER: {self.get_route_name(request)} Wall: {wall:.6f} CPU: {cpu:.6f}"
        logger.info(timing_info)
        return response
