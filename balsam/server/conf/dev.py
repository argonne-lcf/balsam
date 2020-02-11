# flake8: noqa
from .base import *
import os

# SECURITY WARNING! In production, you must:
# 1) Set DEBUG = False
# 2) Have a unique and protected SECRET_KEY (not in source)
# 3) Only include the intended SITENAME in ALLOWED_HOSTS
DEBUG = True
SECRET_KEY = "insecure-key-for-dev"
ALLOWED_HOSTS = ["*"]

DEBUG_TOOLBAR = os.environ.get("DEBUG_TOOLBAR", False)
if DEBUG_TOOLBAR:
    INTERNAL_IPS = ["127.0.0.1"]
    INSTALLED_APPS.append("debug_toolbar")
    MIDDLEWARE.insert(0, "debug_toolbar.middleware.DebugToolbarMiddleware")

db = dict(
    ENGINE="django.db.backends.postgresql",
    NAME="balsam",
    USER="postgres",
    PASSWORD="postgres",
    HOST="localhost",
    PORT=5432,
    CONN_MAX_AGE=60,
    OPTIONS={"connect_timeout": 30, "client_encoding": "UTF8",},
)
DATABASES = {"default": db}
