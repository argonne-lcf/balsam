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
