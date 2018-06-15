import os
import unittest
import subprocess
import time

from django.core.management import call_command
from django import db
from django.conf import settings

from balsam.service.models import BalsamJob, ApplicationDefinition

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
    result = False
    while time.time() - start < timeout:
        result = function(*args)
        if result: break
        else: time.sleep(period)
    return result

def create_job(*, name='', app='', direct_command='', site=settings.BALSAM_SITE, num_nodes=1,
               ranks_per_node=1, args='', workflow='', envs={}, state='CREATED',
               url_in='', input_files='', url_out='', stage_out_files='', 
               post_error_handler=False, post_timeout_handler=False,
               auto_timeout_retry=True, preproc='', postproc='', wtime=1):

    if app and direct_command:
        raise ValueError("Cannot have both application and direct command")

    job = BalsamJob()
    job.name = name
    job.application = app
    job.direct_command = direct_command
    
    job.allowed_work_sites = site
    
    job.num_nodes = num_nodes
    job.ranks_per_node = ranks_per_node
    job.application_args = args
    
    job.workflow = workflow
    job.environ_vars = ':'.join(f'{k}={v}' for k,v in envs.items())
    job.state = state

    job.stage_in_url = url_in
    job.input_files = input_files
    job.stage_out_url = url_out
    job.stage_out_files = stage_out_files

    job.post_error_handler = post_error_handler
    job.post_timeout_handler = post_timeout_handler
    job.auto_timeout_retry = auto_timeout_retry

    job.preprocess = preproc
    job.postprocess = postproc
    job.wall_time_minutes = wtime
    
    job.save()
    job.create_working_path()
    return job

def create_app(*, name='', description='', executable='', preproc='',
               postproc='', envs={}):

    app = ApplicationDefinition()
    app.name = name
    app.description = description
    app.executable = executable
    app.default_preprocess = preproc
    app.default_postprocess = postproc
    app.environ_vars = ':'.join(f'{k}={v}' for k,v in envs.items())
    app.save()
    return app
