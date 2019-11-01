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
import json
import os
import uuid 
from collections import deque

from balsam import setup
setup()
from balsam.core.models import BalsamJob, history_line
from balsam.service.schedulers import JobEnv

__all__ = ['JOB_ID', 'TIMEOUT', 'ERROR', 
           'current_job', 'parents', 'children',
           'add_job', 'add_dependency', 'spawn_child',
           'kill']

current_job = None
parents = None
children = None
_envs = {k:v for k,v in os.environ.items() if k.find('BALSAM')>=0}

JOB_ID = _envs.get('BALSAM_JOB_ID', '')
TIMEOUT = _envs.get('BALSAM_JOB_TIMEOUT', False) == "TRUE"
ERROR = _envs.get('BALSAM_JOB_ERROR', False) == "TRUE"
LAUNCHER_NODES = JobEnv.num_workers
if LAUNCHER_NODES is None: 
    LAUNCHER_NODES = 1

if JOB_ID:
    JOB_ID = uuid.UUID(JOB_ID)
    try:
        current_job = BalsamJob.objects.get(pk=JOB_ID)
    except:
        raise RuntimeError(f"The environment specified current job: "
                           f"BALSAM_JOB_ID {JOB_ID}\n but this does not "
                           f"exist in DB! Was it deleted accidentally?"
                           "You may need to `unset BALSAM_JOB_ID`")
    else:
        parents = current_job.get_parents()
        children = current_job.get_children()

def add_job(
        name, workflow, application, 
        description='', args='',
        num_nodes=1, ranks_per_node=1, 
        cpu_affinity='depth', threads_per_rank=1,
        threads_per_core=1, 
        environ_vars={}, 
        data=None,
        save=True,
        **kwargs
    ):
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
    job.name             = name
    job.workflow         = workflow
    job.application      = application
    job.description      = description
    job.args             = args
    job.num_nodes        = num_nodes
    job.ranks_per_node   = ranks_per_node
    job.threads_per_rank = threads_per_rank
    job.threads_per_core = threads_per_core
    job.cpu_affinity   = cpu_affinity
    job.environ_vars   = environ_vars
    job.data = data if data else dict()
    job.get_application()

    for k,v in kwargs.items():
        setattr(job, k, v)

    if current_job:
        job.queued_launch = current_job.queued_launch
    if save:
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

def breadth_first_iterator(roots, max_depth=None):
    try: 
        roots = iter(roots)
    except TypeError:
        assert isinstance(roots, BalsamJob)
        roots = [roots]

    queue = deque(roots)
    visited = []
    while queue:
        pass

def wf_from_template(source_wf, new_wf, **kwargs):
    source_jobs = BalsamJob.objects.filter(workflow=source_wf)
    if not source_jobs.exists():
        raise ValueError(f"No jobs exist matching workflow {source_wf}; \
                please provide an existing name for source_wf")
    if BalsamJob.objects.filter(workflow=new_wf).exists():
        raise ValueError(f"At least one job with workflow {new_wf} already exists;\
                please provide a unique name for new_wf")

    source_jobs = list(source_jobs)
    new_jobs = {}

    for job in source_jobs:
        kw = kwargs.get(job.name, {})
        kw.update(name=job.name, workflow=new_wf, queued_launch=None, parents="[]")
        new_job = clone(job, **kw)
        assert new_job.name not in new_jobs
        new_jobs[new_job.name] = new_job

    for j in new_jobs.values():
        j.state_history = history_line("CREATED", f"Copied from template workflow {source_wf}")
        j.save()

    for old_job in source_jobs:
        new_job = new_jobs[old_job.name]
        parent_names = old_job.get_parents().values_list('name', flat=True)
        for parent_name in parent_names:
            new_parent = new_jobs[parent_name]
            add_dependency(parent=new_parent, child=new_job)

    return BalsamJob.objects.filter(workflow=new_wf)

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
    from django.db.models.query import QuerySet
    if isinstance(parent, str):
        parent = uuid.UUID(parent)
        parent = BalsamJob.objects.get(pk=parent)
    elif isinstance(parent, uuid.UUID):
        parent = BalsamJob.objects.get(pk=parent)
    elif isinstance(parent, QuerySet): 
        assert parent.count() == 1
        parent = parent.first()
    else:
        assert isinstance(parent, BalsamJob)

    if isinstance(child, str): 
        child = uuid.UUID(child)
        child = BalsamJob.objects.get(pk=child)
    elif isinstance(child, uuid.UUID):
        child = BalsamJob.objects.get(pk=child)
    elif isinstance(child, QuerySet): 
        assert child.count() == 1
        child = child.first()
    else:
        assert isinstance(child, BalsamJob)

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

def clone(job, **kwargs):
    assert isinstance(job, BalsamJob)
    new_job = BalsamJob()
    
    exclude_fields = '''_state objects source state tick user_workdir
    lock state_history job_id'''.split()
    fields = [f for f in job.__dict__ if f not in exclude_fields]

    for f in fields: 
        new_job.__dict__[f] = job.__dict__[f]
    assert new_job.pk != job.pk
    
    for k,v in kwargs.items():
        try: field = job._meta.get_field(k)
        except: raise ValueError(f"Invalid field name: {k}")
        else: new_job.__dict__[k] = v
    return new_job

def spawn_child(**kwargs):
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
    
    child = clone(current_job, **kwargs)
    child.queued_launch = current_job.queued_launch

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

def get_database_paths(verbose=True):
    """
    Prints the paths for existing balsam databases
    """
    try:
        from balsam.django_config.db_index import refresh_db_index
        databasepaths = refresh_db_index()
    except:
        databasepaths = None
    if verbose:
        if len(databasepaths) > 0:
            print(f'Found {len(databasepaths)} balsam database location')
            for db in databasepaths:
                print(db)
        else:
            print('No balsam database found')
    return databasepaths

def get_active_database(verbose=True):
    """
    Gets the activate database set in environment variable BALSAM_DB_PATH
    Parameters:
    verbose: Boolean, (True): Prints verbose info (False): No print
    Returns
    -------
    str, path for the active database
    """
    db = os.environ.get("BALSAM_DB_PATH")
    if verbose: print(f'Active balsam database path: {db}')
    return db

def add_app(name, executable, description='', envscript='', preprocess='', postprocess='', checkexe=False):
    """
    Adds a new app to the balsam database.
    """
    from balsam.core.models import ApplicationDefinition as App
    import shutil
    
    if checkexe and not shutil.which(executable):        
        raise ValueError('No executable {} found in the PATH'.format(executable))

    newapp, created = App.objects.get_or_create(name=name)
    newapp.name        = name
    newapp.executable  = executable
    newapp.description = description
    newapp.envscript   = envscript
    newapp.preprocess  = preprocess
    newapp.postprocess = postprocess
    newapp.save()
    if created: print("Created new app")
    else: print("Updated existing app")
    return newapp

def get_apps(verbose=True):
    """
    Returns all apps as a list
    """
    try:
        from balsam.core.models import ApplicationDefinition as App
        apps = App.objects.all()
    except:
        apps = None
    return apps

def submit(project='datascience',queue='debug-flat-quad',nodes=1,wall_minutes=30,job_mode='mpi',wf_filter=''):
    """
    Submits a job to the queue with the given parameters.
    Parameters
    ----------
    project: str, name of the project to be charged
    queue: str, queue name, can be: 'default', 'debug-cache-quad', or 'debug-flat-quad'
    nodes: int, Number of nodes, can be any integer from 1 to 4096.
    wall_minutes: int, max wall time in minutes
    job_mode: str, Balsam job mode, can be 'mpi', 'serial'
    wf_filter: str, Selects Balsam jobs that matches the given workflow filter.
    """
    from balsam.service import service
    from balsam.core import models
    QueuedLaunch = models.QueuedLaunch
    mylaunch = QueuedLaunch()
    mylaunch.project = project
    mylaunch.queue = queue
    mylaunch.nodes = nodes
    mylaunch.wall_minutes = wall_minutes
    mylaunch.job_mode = job_mode
    mylaunch.wf_filter = wf_filter
    mylaunch.prescheduled_only=False
    mylaunch.save()
    service.submit_qlaunch(mylaunch, verbose=True)
