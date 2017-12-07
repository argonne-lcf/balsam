import os
import unittest
import subprocess
import time

from django.core.management import call_command
from django import db
from django.conf import settings

class BalsamTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        assert db.connection.settings_dict['NAME'].endswith('test_db.sqlite3')
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

def cmdline(cmd,envs=None,shell=True):
    '''Return string output from a command line'''
    p = subprocess.Popen(cmd, shell=shell, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT,env=envs)
    return p.communicate()[0].decode('utf-8')

def poll_until_returns_true(function, *, args=(), period=1.0, timeout=12.0):
    start = time.time()
    while time.time() - start < timeout:
        result = function(*args)
        if result: break
        else: time.sleep(period)
    return result
