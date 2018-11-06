import json
import os
import sys
import shutil
import tempfile
from balsam.django_config.serverinfo import ServerInfo

home_dir = os.path.expanduser('~')
BALSAM_HOME = os.path.join(home_dir, '.balsam')

def bootstrap():
    if not os.path.exists(BALSAM_HOME):
        os.makedirs(BALSAM_HOME, mode=0o755)

    sys.path.append(BALSAM_HOME)

    migrations_path = os.path.join(BALSAM_HOME, 'balsamdb_migrations')
    user_settings_path = os.path.join(BALSAM_HOME, 'settings.json')
    user_policy_path = os.path.join(BALSAM_HOME, 'theta_policy.ini')
    user_templates_path = os.path.join(BALSAM_HOME, 'job-templates')
    here = os.path.dirname(os.path.abspath(__file__))

    default_settings_path = os.path.join(here, 'default_settings.json')
    default_settings = json.load(open(default_settings_path))

    if not os.path.exists(user_settings_path):
        shutil.copy(default_settings_path, user_settings_path)
        print("Set up your Balsam config directory at", BALSAM_HOME)
    if not os.path.exists(user_policy_path):
        default_policy_path = os.path.join(here, 'theta_policy.ini')
        shutil.copy(default_policy_path, user_policy_path)
    if not os.path.exists(user_templates_path):
        default_templates_path = os.path.join(here, 'job-templates')
        shutil.copytree(default_templates_path, user_templates_path)
    if not os.path.exists(migrations_path):
        os.makedirs(migrations_path, mode=0o755)
        with open(os.path.join(migrations_path, '__init__.py'), 'w') as fp:
            fp.write('\n')
    try:
        user_settings = json.load(open(user_settings_path))
        for key in default_settings: assert key in user_settings
    except (AssertionError, json.decoder.JSONDecodeError):
        print(f"Detected invalid settings in {user_settings_path}; replacing with defaults!")
        shutil.copy(default_settings_path, user_settings_path)
        user_settings = json.load(open(user_settings_path))

    thismodule = sys.modules[__name__]
    for k, v in user_settings.items():
        setattr(thismodule, k, v)

def resolve_db_path():
    path = os.environ.get('BALSAM_DB_PATH', '')
    if path: path = os.path.abspath(os.path.expanduser(path))
    if not os.path.isdir(path):
        sys.stderr.write(f"Please use `source balsamactivate` to set BALSAM_DB_PATH to a valid location\n")
        sys.stderr.write(f"  --> `balsam which --list` recalls visited DBs in the filesystem\n")
        sys.stderr.write(f"Or, use `balsam init` to start a new Balsam DB\n")
        sys.exit(1)
    os.environ['BALSAM_DB_PATH'] = path
    return path

if os.environ.get('BALSAM_SPHINX_DOC_BUILD_ONLY', False):
    tempdir = tempfile.TemporaryDirectory()
    BALSAM_PATH = tempdir.name
    print("detected env BALSAM_SPHINX_DOC_BUILD_ONLY: will not connect to a real DB")
else:
    BALSAM_PATH = resolve_db_path()

bootstrap()
DATABASES = ServerInfo(BALSAM_PATH).django_db_config()

# SUBDIRECTORY SETUP
# ------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOGGING_DIRECTORY = os.path.join(BALSAM_PATH , 'log') 
DATA_PATH = os.path.join(BALSAM_PATH ,'data')
SERVICE_PATH = os.path.join(BALSAM_PATH ,'qsubmit')
BALSAM_WORK_DIRECTORY = DATA_PATH

for d in [
      BALSAM_PATH ,
      DATA_PATH,
      LOGGING_DIRECTORY,
      SERVICE_PATH
]:
    if not os.path.exists(d):
        os.makedirs(d)

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = '=gyp#o9ac0@w3&-^@a)j&f#_n-o=k%z2=g5u@z5+klmh_*hebj'

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

ALLOWED_HOSTS = []

# Application definition

INSTALLED_APPS = [
    'balsam.core.apps.BalsamCoreConfig',
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
]

MIGRATION_MODULES = {'core': 'balsamdb_migrations'}

MIDDLEWARE_CLASSES = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.auth.middleware.SessionAuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'balsam.django_config.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'balsam.django_config.wsgi.application'


# Password validation
# https://docs.djangoproject.com/en/1.9/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


# Internationalization
# https://docs.djangoproject.com/en/1.9/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_L10N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/1.9/howto/static-files/

STATIC_URL = '/static/'
STATIC_ROOT = os.path.join(BASE_DIR, 'static')
