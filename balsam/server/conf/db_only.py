import os
import sys
import django

LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_L10N = True
USE_TZ = True

MIGRATION_MODULES = {}

INSTALLED_APPS = [
    'balsam.server.BalsamAppConfig',
]
SECRET_KEY = 'not-secret'
DATABASES = None

def set_database(
    user,
    passwd,
    host,
    port,
    db_name="balsam",
    engine='django.db.backends.postgresql',
    conn_max_age=60,
    db_options={
        'connect_timeout': 30,
        'client_encoding': 'UTF8',
        'default_transaction_isolation': 'read committed',
        'timezone': 'UTC',
    }
):
    global DATABASES
    db = dict(
        ENGINE=engine,
        NAME=db_name,
        OPTIONS=db_options,
        USER=user,
        PASSWORD=passwd,
        HOST=host,
        PORT=port,
        CONN_MAX_AGE=conn_max_age,
    )
    DATABASES = {'default': db}
    os.environ.setdefault(
        'DJANGO_SETTINGS_MODULE',
        'balsam.server.conf.db_only'
    )
    django.setup()
