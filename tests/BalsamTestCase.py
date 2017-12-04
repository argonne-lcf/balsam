import os
import unittest

from django.core.management import call_command
from django import db
from django.conf import settings

class BalsamTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        call_command('makemigrations',interactive=False,verbosity=0)
        call_command('migrate',interactive=False,verbosity=0)
        assert os.path.exists(settings.DATABASES['default']['NAME'])

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        pass # to be implemented by test cases

    def tearDown(self):
        if not db.connection.settings_dict['NAME'].endswith('test_db.sqlite3'):
            raise RuntimeError("Test DB not configured")
        call_command('flush',interactive=False,verbosity=0)
