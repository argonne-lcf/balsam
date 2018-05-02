''' API for BalsamJob database (DAG) Manipulations
==================================================

The ``launcher.dag`` module provides a number of convenient environment
variables and functions that allow you to quickly write pre- and post-processing
scripts that interact with the BalsamJob database.

When pre- or post-processing steps for a job occur, the Launcher runs your
scripts in the job's working directory with some job-specific environment
variables. If you choose to write these scripts in Python with ``launcher.dag``,
then these variables are automatically loaded for you on module import.

Useful module-level attributes
-------------------------------

``current_job``
    The BalsamJob object which is currently being processed

``parents``
    Parent BalsamJob objects on which current_job depends

``children``
    Children BalsamJob objects that depend on current_job

``JOB_ID``
    Unique identifier and primary key of the BalsamJob

``TIMEOUT``
    Boolean flag indicating whether the current job has timed-out while running
    in the Launcher.  If True, Balsam has invoked the your script as a
    timeout-handler, and the script should take some clean-up or rescue acction
    such as spawning a new job.

``ERROR``
    Boolean flag indicating whether the current job's application returned a
    nonzero exit code to the Launcher.  If True, Balsam has invoked your
    script as an error-handler, and the script should take some clean-up or
    rescue action.

Usage example
--------------
A typical user's post-processing script might import and use the ``dag`` API as
follows::

     import balsam.launcher.dag as dag

     output = open('expected_output').read() # in job workdir

     if 'CONVERGED' not in output:
         # Kill subtree of this job
         for child in dag.children:
             dag.kill(child, recursive=True)

         # Create a child clone job with increased walltime and new input
         with open("input_rescue.dat", 'w') as fp:
             fp.write("# a new input file here")

         dag.spawn_child(clone=True,
             walltime_minutes=dag.current_job.walltime_minutes + 10, 
             input_files = 'input_rescue.dat')
'''
    
import django
import json
import os
import uuid 

__all__ = ['JOB_ID', 'TIMEOUT', 'ERROR', 
           'current_job', 'parents', 'children',
           'add_job', 'add_dependency', 'spawn_child',
           'kill']

os.environ['DJANGO_SETTINGS_MODULE'] = 'balsam.django_config.settings'
django.setup()

from balsam.service.models import BalsamJob, history_line
from django.conf import settings

current_job = None
parents = None
children = None

_envs = {k:v for k,v in os.environ.items() if k.find('BALSAM')>=0}

JOB_ID = _envs.get('BALSAM_JOB_ID', '')
TIMEOUT = _envs.get('BALSAM_JOB_TIMEOUT', False) == "TRUE"
ERROR = _envs.get('BALSAM_JOB_ERROR', False) == "TRUE"

if JOB_ID:
    JOB_ID = uuid.UUID(JOB_ID)
    try:
        current_job = BalsamJob.objects.get(pk=JOB_ID)
    except:
        raise RuntimeError(f"The environment specified current job: "
                           f"BALSAM_JOB_ID {JOB_ID}\n but this does not "
                           f"exist in DB! Was it deleted accidentally?")
    else:
        parents = current_job.get_parents()
        children = current_job.get_children()

def add_job(**kwargs):
    '''Add a new job to the BalsamJob DB
    
    Creates a new job and saves it to the database in CREATED state.
    The job is initialized with all blank/default values for its fields; these
    must be configured by the user or provided via ``kwargs``
    
    Args:
        - ``kwargs`` (*dict*): contains BalsamJob fields (keys) and their values to
          be set on BalsamJob instantiation.

    Returns:
        - ``job`` (*BalsamJob*): the newly-created BalsamJob instance

    Raises:
        - ``ValueError``: if an invalid field name is provided to *kwargs*
    '''
    job = BalsamJob()
    for k,v in kwargs.items():
        try:
            getattr(job, k)
        except AttributeError: 
            raise ValueError(f"Invalid field {k}")
        else:
            setattr(job, k, v)
    if 'allowed_work_sites' not in kwargs:
        job.allowed_work_sites = settings.BALSAM_SITE
    job.save()
    return job

def detect_circular(job, path=[]):
    '''Detect a circular dependency in DAG
    
    Args:
        - ``job`` (*BalsamJob*): node at which to start traversing the DAG
    
    Returns:
        - ``detected`` (*bool*): True if a circular dependency was detected
    ''' 
    if job.pk in path: return True
    path = path[:] + [job.pk]
    for parent in job.get_parents():
        if detect_circular(parent, path): return True
    return False

def add_dependency(parent,child):
    '''Create a dependency between two existing jobs
    
    Args:
        - ``parent`` (*BalsamJob*): The job which must reach state JOB_FINISHED
          before ``child`` begins processing
        - ``child`` (*BalsamJob*): The job that is dependent on ``parent`` for
          control- and/or data-flow.

    Raises:
        - ``RuntimeError``: if the attempted edge would create a circular
          dependency in the BalsamJob DAG.
    '''
    if isinstance(parent, str): parent = uuid.UUID(parent)
    if isinstance(child, str): child = uuid.UUID(child)

    if not isinstance(parent, BalsamJob): 
        parent = BalsamJob.objects.get(pk=parent)
    if not isinstance(child, BalsamJob): 
        child = BalsamJob.objects.get(pk=child)

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
    '''Add a new job that is dependent on the current job
    
    This function creates a new child job that will not start until the current
    job is finished processing. The job is added to the BalsamJob database in
    CREATED state.

    Args:
        - ``clone`` (*bool*): If *True*, all fields of the current BalsamJob are
          copied into the child job (except for primary key and working
          directory). Specific fields may then be overwritten via *kwargs*. 
          Defaults to *False*.
        - ``kwargs`` (*dict*) : Contains BalsamJob field names as keys and their
          desired values.

    Returns:
        - ``child`` (BalsamJob): returns the newly created BalsamJob

    Raises:
        - ``RuntimeError``: If no BalsamJob detected on module-load
        - ``ValueError``: if an invalid field name is passed into *kwargs* 
    '''
    if not isinstance(current_job, BalsamJob):
        raise RuntimeError("No current BalsamJob detected in environment")

    if 'workflow' not in kwargs:
        kwargs['workflow'] = current_job.workflow
    
    if 'allowed_work_sites' not in kwargs:
        kwargs['allowed_work_sites'] = settings.BALSAM_SITE

    child = BalsamJob()
    new_pk = child.pk

    exclude_fields = '_state version state_history job_id working_directory'.split()
    fields = [f for f in current_job.__dict__ if f not in exclude_fields]

    if clone:
        for f in fields: 
            child.__dict__[f] = current_job.__dict__[f]
        assert child.pk == new_pk

    for k,v in kwargs.items():
        if k in fields:
            child.__dict__[k] = v
        else:
            raise ValueError(f"Invalid field {k}")

    #child.working_directory = '' # working directory is computed property instead

    newparents = json.loads(current_job.parents)
    newparents.append(str(current_job.job_id))
    child.parents = json.dumps(newparents)
    child.state = "CREATED"
    child.state_history = history_line("CREATED", f"spawned by {current_job.cute_id}")
    child.save()
    return child

def kill(job, recursive=True):
    '''Kill a job or its entire subtree in the DAG

    Mark a job (and optionally all jobs that depend on it) by the state
    USER_KILLED, which will prevent any further processing.
    
    Args:
        - ``job`` (*BalsamJob*): the job (or subtree root) to kill
        - ``recursive`` (*bool*): if *True*, then traverse the DAG recursively
          to kill all jobs in the subtree. Defaults to *True*
    '''
    job.update_state('USER_KILLED')
    if recursive:
        for child in job.get_children():
            kill(child, recursive=True)
