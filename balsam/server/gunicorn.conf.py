import os
from pathlib import Path

LOG_DIR = Path(os.environ.setdefault("BALSAM_LOG_DIR", os.getcwd()))
SERVER_PORT = os.environ.get("SERVER_PORT", "8000")

# Workers
workers = 1
timeout = 60
worker_class = "uvicorn.workers.UvicornWorker"
bind = f"0.0.0.0:{SERVER_PORT}"

# Logging
loglevel = "info"
accesslog = (LOG_DIR / "gunicorn.access").as_posix()
errorlog = (LOG_DIR / "gunicorn.error").as_posix()
access_log_format = repr(
    {"remote": "%(h)s", "date": "%(t)s", "request": "%(r)s", "status": "%(s)s", "response_sec": "%(L)s"}
)
logger_class = "balsam.server.gunicorn_logger.RotatingGunicornLogger"
capture_output = True

# Server Mechanics
proc_name = "balsam-server"
pidfile = "gunicorn.pid"
