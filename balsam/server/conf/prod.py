import os
from .base import *

# SECURITY WARNING! In production, you must:
# 1) Set DEBUG = False
# 2) Have a unique and protected SECRET_KEY (not in source)
# 3) Only include the intended SITENAME in ALLOWED_HOSTS
DEBUG = False
SECRET_KEY = os.environ['DJANGO_SECRET_KEY']
ALLOWED_HOSTS = [os.environ['SITENAME']]

log_path = os.path.join(BASE_DIR, 'django.log')
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '%(asctime)s %(levelname)s [%(name)s:%(lineno)s] %(module)s %(process)d %(thread)d %(message)s'
        }
    },
    'handlers': {
        'file': {
            'level': 'DEBUG',
            'class': 'logging.handlers.RotatingFileHandler',
            'formatter': 'verbose',
            'filename': log_path,
            'maxBytes': 1024 * 1024 * 100,  # 100 mb
        }
    },
    'loggers': {
        'gunicorn': {
            'level': 'DEBUG',
            'handlers': ['file'],
            'propagate': True,
        },
        'django': {
            'level': 'DEBUG',
            'handlers': ['file'],
            'propagate': True,
        },
    }
}