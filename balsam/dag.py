'''Python API for Balsam DAG Manipulations

Example usage:
>>>     import balsam.dag as dag
>>>
>>>     output = open('expected_output').read()
>>>
>>>     if 'CONVERGED' not in output:
>>>         for child in dag.children:
>>>             dag.kill(child, recursive=True)
>>>
>>>         with open("data/input.dat", 'w') as fp:
>>>             fp.write("# a new input file here")
>>>
>>>         dag.spawn_child(clone=dag.current_job,
>>>             walltime_minutes=dag.current_job.walltime_minutes + 10, 
>>>             input_files = 'input.dat')
>>>
'''
    
import django as _django
import os as _os
import uuid 

__all__ = ['JOB_ID', 'TIMEOUT', 'ERROR', 
           'current_job', 'parents', 'children',
           'add_job', 'add_dependency', 'spawn_child',
           'kill']

_os.environ['DJANGO_SETTINGS_MODULE'] = 'argobalsam.settings'
_django.setup()

from django.conf import settings
from balsam.models import BalsamJob as _BalsamJob

x = _BalsamJob()
assert isinstance(x, _BalsamJob)

_envs = {k:v for k,v in _os.environ.items() if k.find('BALSAM')>=0}


current_job = None
parents = None
children = None

JOB_ID = _envs.get('BALSAM_JOB_ID', '')
TIMEOUT = bool(_envs.get('BALSAM_JOB_TIMEOUT', False))
ERROR = bool(_envs.get('BALSAM_JOB_ERROR', False))

if JOB_ID:
    JOB_ID = uuid.UUID(JOB_ID)
    current_job = _BalsamJob.objects.get(pk=JOB_ID)
    parents = current_job.get_parents()
    children = curren_job.get_children()


def add_job(**kwargs):
    '''Add a new job to BalsamJob DB'''
    job = _BalsamJob()
    for k,v in kwargs.items():
        try:
            getattr(job, k)
        except AttributeError: 
            raise
        else:
            setattr(job, k, v)
    job.save()
    return job

def add_dependency(parent,child):
    '''Create a dependency between two existing jobs'''
    if isinstance(parent, str): parent = uuid.UUID(parent)
    if isinstance(child, str): child = uuid.UUID(child)

    if not isinstance(parent, _BalsamJob): 
        parent = _BalsamJob.objects.get(pk=parent)
    if not isinstance(child, _BalsamJob): 
        child = _BalsamJob.objects.get(pk=child)

    new_parents = child.get_parents_by_id()
    new_parents.append(str(parent.pk))
    child.parents.set_parents(new_parents)

def spawn_child(**kwargs):
    '''Add new job that is dependent on the current job'''
    child = add_job(**kwargs)
    add_dependency(current_job, child)
    return child

def kill(job, recursive=False):
    '''Kill a job or its entire subtree in the DAG'''
    job.update_state('USER_KILLED')
    if recursive:
        for child in job.get_children():
            kill(child, recursive=True)
