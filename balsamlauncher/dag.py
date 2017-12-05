'''Python API for Balsam DAG Manipulations

Example usage:
>>>     import balsamlauncher.dag as dag
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
    
import django as django
import os as os
import uuid 

__all__ = ['JOB_ID', 'TIMEOUT', 'ERROR', 
           'current_job', 'parents', 'children',
           'add_job', 'add_dependency', 'spawn_child',
           'kill']

os.environ['DJANGO_SETTINGS_MODULE'] = 'argobalsam.settings'
django.setup()

from balsam.models import BalsamJob as _BalsamJob

current_job = None
parents = None
children = None

_envs = {k:v for k,v in os.environ.items() if k.find('BALSAM')>=0}

JOB_ID = _envs.get('BALSAM_JOB_ID', '')
TIMEOUT = bool(_envs.get('BALSAM_JOB_TIMEOUT', False))
ERROR = bool(_envs.get('BALSAM_JOB_ERROR', False))

if JOB_ID:
    JOB_ID = uuid.UUID(JOB_ID)
    try:
        current_job = _BalsamJob.objects.get(pk=JOB_ID)
    except:
        raise RuntimeError(f"The environment specified current job: "
                           "BALSAM_JOB_ID {JOB_ID}\n but this does not "
                           "exist in DB! Was it deleted accidentally?")
    else:
        parents = current_job.get_parents()
        children = current_job.get_children()


def add_job(**kwargs):
    '''Add a new job to BalsamJob DB'''
    job = _BalsamJob()
    for k,v in kwargs.items():
        try:
            getattr(job, k)
        except AttributeError: 
            raise ValueError(f"Invalid field {k}")
        else:
            setattr(job, k, v)
    job.save()
    return job

def detect_circular(job, path=[]):
    if job.pk in path: return True
    path = path[:] + [job.pk]
    for parent in job.get_parents():
        if detect_circular(parent, path): return True
    return False

def add_dependency(parent,child):
    '''Create a dependency between two existing jobs'''
    if isinstance(parent, str): parent = uuid.UUID(parent)
    if isinstance(child, str): child = uuid.UUID(child)

    if not isinstance(parent, _BalsamJob): 
        parent = _BalsamJob.objects.get(pk=parent)
    if not isinstance(child, _BalsamJob): 
        child = _BalsamJob.objects.get(pk=child)

    existing_parents = child.get_parents_by_id()
    new_parents = existing_parents.copy()
    parent_pk_str = str(parent.pk)
    if parent_pk_str in existing_parents:
        raise RuntimeError("Dependency already exists; cannot double-create")
    else:
        new_parents.append(parent_pk_str)
    child.set_parents(new_parents)
    if detect_circular(child):
        child.set_parents(existing_parents)
        raise RuntimeError("Detected circular dependency; not creating link")

def spawn_child(clone=False, **kwargs):
    '''Add new job that is dependent on the current job'''
    if not isinstance(current_job, _BalsamJob):
        raise RuntimeError("No current BalsamJob detected in environment")
    if clone:
        child = _BalsamJob.objects.get(pk=current_job.pk)
        child.pk = None
        for k,v in kwargs.items():
            try:
                getattr(child, k)
            except AttributeError: 
                raise ValueError(f"Invalid field {k}")
            else:
                setattr(child, k, v)
        child.save()
    else:
        child = add_job(**kwargs)

    add_dependency(current_job, child)
    return child

def kill(job, recursive=False):
    '''Kill a job or its entire subtree in the DAG'''
    job.update_state('USER_KILLED')
    if recursive:
        for child in job.get_children():
            kill(child, recursive=True)
