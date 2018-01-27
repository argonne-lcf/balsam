import os
import sys
import django

os.environ['DJANGO_SETTINGS_MODULE'] = 'balsam.django_config.settings'
django.setup()

from django import db
from django.core.management import call_command
from django.conf import settings

db_path = db.connection.settings_dict['NAME']
print(f"Setting up new balsam database: {db_path}")
call_command('makemigrations', interactive=False, verbosity=2)
call_command('migrate', interactive=False, verbosity=2)

new_path = settings.DATABASES['default']['NAME']
if os.path.exists(new_path):
    print(f"Set up new DB at {new_path}")
else:
    raise RuntimeError(f"Failed to created DB at {new_path}")
