import os
import json
import logging
import re
import socket
import sys
import threading
from datetime import datetime, timedelta
from django.utils import timezone
import uuid

from django.core.exceptions import ValidationError,ObjectDoesNotExist
from django.conf import settings
from django.db import models, transaction
from django.db.models import Value as V
from django.db.models import Q
from django.db.models.functions import Concat


logger = logging.getLogger('balsam.service.models')

class InvalidStateError(ValidationError): pass
class InvalidParentsError(ValidationError): pass
class NoApplication(Exception): pass

TIME_FMT = '%m-%d-%Y %H:%M:%S.%f'

STATES = '''
CREATED
AWAITING_PARENTS
READY

STAGED_IN
PREPROCESSED

RUNNING
RUN_DONE

POSTPROCESSED
JOB_FINISHED

RUN_TIMEOUT
RUN_ERROR
RESTART_READY

FAILED
USER_KILLED
'''.split()

ACTIVE_STATES = '''
RUNNING
'''.split()

PROCESSABLE_STATES = '''
CREATED
AWAITING_PARENTS
READY
STAGED_IN
RUN_DONE
POSTPROCESSED
RUN_TIMEOUT
RUN_ERROR
'''.split()

RUNNABLE_STATES = '''
PREPROCESSED
RESTART_READY
'''.split()

END_STATES = '''
JOB_FINISHED
FAILED
USER_KILLED
'''.split()
        
STATE_TIME_PATTERN = re.compile(r'''
^                  # start of line
\[                 # opening square bracket
(\d+-\d+-\d\d\d\d  # date MM-DD-YYYY
\s+                # one or more space
\d+:\d+:\d+\.\d+)  # time HH:MM:SS.MICROSEC
\s+                # one or more space
(\w+)              # state
\s*                # 0 or more space
\]                 # closing square bracket
''', re.VERBOSE | re.MULTILINE)

_app_cache = {}

def process_job_times(time0=None, state0=None):
    '''Returns {state : [elapsed_seconds_for_each_job_to_reach_state]}
    Useful for tracking job performance/throughput'''
    from collections import defaultdict

    if state0 is None: state0 = 'READY'
    data = BalsamJob.objects.values_list('state_history', flat=True)
    data = '\n'.join(data)
    matches = STATE_TIME_PATTERN.finditer(data)
    result = ( m.groups() for m in matches )
    result = ( (state, datetime.strptime(time_str, TIME_FMT))
              for (time_str, state) in result )
    
    time_data = defaultdict(list)
    for state, time in result:
        time_data[state].append(time)

    if time0 is None: 
        if state0 not in time_data:
            raise ValueError(f"Requested time-zero at first instance of {state0}, "
                "but there are no jobs in the DB with this state!")
        time0 = min(time_data[state0])

    for state in time_data.keys():
        time_data[state] = [(t - time0).total_seconds() for t in sorted(time_data[state])]

    return time_data


def assert_disjoint():
    groups = [ACTIVE_STATES, PROCESSABLE_STATES, RUNNABLE_STATES, END_STATES]
    joined = [state for g in groups for state in g]
    assert len(joined) == len(set(joined)) == len(STATES)
    assert set(joined) == set(STATES) 
    from itertools import combinations
    for g1,g2 in combinations(groups, 2):
        s1,s2 = set(g1), set(g2)
        assert s1.intersection(s2) == set()
assert_disjoint()

def validate_state(value):
    if value not in STATES:
        raise InvalidStateError(f"{value} is not a valid state in balsam.models")

def get_time_string():
    return timezone.now().strftime(TIME_FMT)

def from_time_string(s):
    return datetime.strptime(s, TIME_FMT)

def history_line(state='CREATED', message=''):
    return f"\n[{get_time_string()} {state}] ".rjust(46) + message


class QueuedLaunch(models.Model):

    ADVISORY_LOCK_ID = 1
    scheduler_id = models.IntegerField(primary_key=True)
    queue = models.TextField()
    nodes = models.IntegerField()
    wall_minutes = models.IntegerField()
    job_mode = models.TextField()
    wf_filter = models.TextField()
    serial_jobs_per_node = models.IntegerField(default=1)
    state = models.TextField(default='pending-submission')

    @classmethod
    def acquire_advisory(self):
        from django.db import connection
        with connection.cursor() as cursor:
            command = f"SELECT pg_try_advisory_lock({self.ADVISORY_LOCK_ID})"
            cursor.execute(command)
            row = cursor.fetchone()
        row = ' '.join(map(str, row)).strip().lower()
        if 'true' in row:
            return True
        else:
            return False

    def __repr__(self):
        return f'''Qlaunch<queue {self.queue}, {self.nodes} nodes, {self.wall_minutes} minutes, job-mode:{self.job_mode}, schedulerID:{self.scheduler_id}, state:{self.state}>'''

class JobSource(models.Manager):

    TICK_PERIOD = timedelta(minutes=1)
    EXPIRATION_PERIOD = timedelta(minutes=3)

    def __init__(self, workflow=None):
        super().__init__()
        self.workflow = workflow
        self._lock_base = None
        self._pid = None
        self.qLaunch = None
        self._checked_qLaunch = False

    def check_qLaunch(self):
        from balsam.service.schedulers import JobEnv
        sched_id = JobEnv.current_scheduler_id
        if sched_id is not None:
            try:
                self.qLaunch = QueuedLaunch.objects.get(scheduler_id=sched_id)
            except ObjectDoesNotExist:
                self.qLaunch = None
        self._checked_qLaunch = True

    @property
    def lock_base(self):
        pid = os.getpid()
        if pid != self._pid:
            self._lock_base = self.get_lock_str(base_only=True)
            self._pid = pid
        return self._lock_base

    @property
    def lockQuery(self):
        return Q(lock='') | Q(lock__startswith=self.lock_base)

    def get_lock_str(self, base_only=False):
        if base_only:
            return f"{socket.gethostname()}:{os.getpid()}"
        else:
            return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4()}"

    def get_queryset(self):
        if not self._checked_qLaunch: self.check_qLaunch()

        queryset = super().get_queryset()
        queryset = queryset.filter(self.lockQuery)
        if self.qLaunch:
            queryset = queryset.filter(queued_launch=self.qLaunch)
        elif self.workflow:
            queryset = queryset.filter(workflow=self.workflow)
        return queryset

    def by_states(self, states):
        if isinstance(states, str):
            states = [states]
        elif isinstance(states, dict):
            states = states.keys()
        return self.get_queryset().filter(state__in=states)

    def get_runnable(self, *, max_nodes, remaining_minutes=0, mpi_only=False,
                     serial_only=False, order_by=None):
        if mpi_only and serial_only:
            raise ValueError("arguments mpi_only and serial_only are mutually exclusive")

        if max_nodes < 1:
            raise ValueError("Must be positive number of nodes")

        if serial_only:
            assert max_nodes == 1

        try:
            remaining_minutes = int(remaining_minutes)
        except:
            remaining_minutes = 24 * 60

        runnable = self.by_states(RUNNABLE_STATES)
        runnable = runnable.filter(wall_time_minutes__lte=remaining_minutes,
                                   num_nodes__lte=max_nodes)
        if serial_only:
            runnable = runnable.filter(num_nodes=1, ranks_per_node=1)
        elif mpi_only:
            mpiquery = Q(num_nodes__gt=1) | Q(ranks_per_node__gt=1)
            runnable = runnable.filter(mpiquery)
        if order_by is not None:
            runnable = runnable.order_by(order_by)
        return runnable

    @transaction.atomic
    def acquire(self, pk_list):
        '''input can be actual list of PKs or a queryset'''
        new_lock = self.get_lock_str()
        to_lock = self.get_queryset().filter(pk__in=pk_list)
        to_lock = to_lock.select_for_update().filter(lock='')
        to_lock.update(lock=new_lock,
                       tick=timezone.now())
        acquired = self.get_queryset().filter(lock=new_lock)
        return acquired

    @transaction.atomic
    def acquire_transitionable(self, max_jobs):
        acquired_jobs = []
        while len(acquired_jobs) < max_jobs:
            processable = self.by_states(PROCESSABLE_STATES).filter(lock='')
            if not processable.exists(): 
                logger.info(f"acquire_transitions: could not acquire {max_jobs}; only got {len(acquired_jobs)}")
                break
            num_needed = max_jobs - len(acquired_jobs)
            batch = self.acquire(processable[:num_needed])
            acquired_jobs.extend(job for job in batch)
        return acquired_jobs

    def start_tick(self):
        t = threading.Timer(self.TICK_PERIOD.total_seconds(), self.start_tick)
        t.daemon = True
        t.start()
        self._tick()

    @transaction.atomic
    def _tick(self):
        now = timezone.now()
        queryset = self.get_queryset()
        my_locked = queryset.filter(lock__startswith=self.lock_base)
        my_locked.update(tick=now)

    @transaction.atomic
    def release(self, pk_list):
        to_unlock = self.get_queryset().filter(pk__in=pk_list)
        to_unlock = to_unlock.select_for_update()
        to_unlock.update(lock='')

    @transaction.atomic
    def release_all_owned(self):
        to_unlock = self.get_queryset()
        to_unlock.filter(lock__startswith=self.lock_base).update(lock='')
    
    @transaction.atomic
    def clear_stale_locks(self):
        objects = self.model.objects
        total_count = objects.count()
        locked_count = objects.exclude(lock='').count()
        logger.info(f'{locked_count} out of {total_count} jobs are locked')

        all_jobs = objects.all().select_for_update()
        expired_time = timezone.now() - self.EXPIRATION_PERIOD
        expired_jobs = all_jobs.exclude(lock='').filter(tick__lte=expired_time)
        revert_count = expired_jobs.filter(state='RUNNING').update(state='RESTART_READY')
        expired_count = expired_jobs.update(lock='')
        if expired_count:
            logger.info(f'Cleared stale lock on {expired_count} jobs')
            if revert_count: logger.info(f'Reverted {revert_count} RUNNING jobs to RESTART_READY')
        elif locked_count:
            logger.info(f'No stale locks (older than {self.EXPIRATION_PERIOD.total_seconds()} seconds)')


class BalsamJob(models.Model):
    ''' A DB representation of a Balsam Job '''

    objects = models.Manager()
    source = JobSource()

    job_id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False)

    workflow = models.TextField(
        'Workflow Name',
        help_text='Name of the workflow to which this job belongs',
        default='')
    name = models.TextField(
        'Job Name',
        help_text='A name for the job given by the user.',
        default='')
    description = models.TextField(
        'Job Description',
        help_text='A description of the job.',
        default='')
    lock = models.TextField(
        'Process Lock',
        help_text='{hostname}:{PID} set by process that currently owns the job',
        default=''
    )
    tick = models.DateTimeField(auto_now_add=True)

    parents = models.TextField(
        'IDs of the parent jobs which must complete prior to the start of this job.',
        default='[]')

    input_files = models.TextField(
        'Input File Patterns',
        help_text="Space-delimited filename patterns that will be searched in the parents'"\
        "working directories. Every matching file will be made available in this"\
        "job's working directory (symlinks for local Balsam jobs, file transfer for"\
        "remote Balsam jobs). Default: all files from parent jobs are made available.",
        default='*')
    stage_in_url = models.TextField(
        'External stage in files or folders', help_text="A list of URLs for external data to be staged in prior to job processing. Job dataflow from parents to children is NOT handled here; see `input_files` field instead.",
        default='')
    stage_out_files = models.TextField(
        'External stage out files or folders',
        help_text="A string of filename patterns. Matches will be transferred to the stage_out_url. Default: no files are staged out",
        default='')
    stage_out_url = models.TextField(
        'Stage Out URL',
        help_text='The URLs to which designated stage out files are sent.',
        default='')

    wall_time_minutes = models.IntegerField(
        'Job Wall Time in Minutes',
        help_text='The number of minutes the job is expected to take',
        default=1)
    num_nodes = models.IntegerField(
        'Number of Compute Nodes',
        help_text='The number of compute nodes requested for this job.',
        default=1)
    coschedule_num_nodes = models.IntegerField(
        'Number of additional compute nodes to reserve alongside this job',
        help_text='''Used by Balsam service only.  If a pilot job runs on one or a
        few nodes, but requires additional worker nodes alongside it,
        use this field to specify the number of additional nodes that will be
        reserved by the service for this job.''',
        default=0)
    ranks_per_node = models.IntegerField(
        'Number of ranks per node',
        help_text='The number of MPI ranks per node to schedule for this job.',
        default=1)
    cpu_affinity = models.TextField(
        'Cray CPU Affinity ("depth" or "none")',
        default="none")
    threads_per_rank = models.IntegerField(
        'Number of threads per MPI rank',
        help_text='The number of OpenMP threads per MPI rank (if applicable)',
        default=1)
    threads_per_core = models.IntegerField(
        'Number of hyperthreads per physical core (if applicable)',
        help_text='Number of hyperthreads per physical core.',
        default=1)
    serial_node_packing_count = models.IntegerField(
        'For serial (non-MPI) jobs only. How many to run concurrently on a node.',
        help_text='Setting this field at 2 means two serial jobs will run at a '
        'time on a node. This field is ignored for MPI jobs.',
        default=1)
    environ_vars = models.TextField(
        'Environment variables specific to this job',
        help_text="Colon-separated list of envs like VAR1=value1:VAR2=value2",
        default='')
    
    application = models.TextField(
        'Application to Run',
        help_text='The application to run; located in Applications database',
        default='')
    application_args = models.TextField(
        'Command-line args to the application exe',
        help_text='Command line arguments used by the Balsam job runner',
        default='')


    wait_for_parents = models.BooleanField(
            'If True, do not process this job until parents are FINISHED',
            default=True)
    post_error_handler = models.BooleanField(
        'Let postprocesser try to handle RUN_ERROR',
        help_text='If true, the postprocessor will be invoked for RUN_ERROR jobs'
        ' and it is up to the script to handle error and update job state.',
        default=False)
    post_timeout_handler = models.BooleanField(
        'Let postprocesser try to handle RUN_TIMEOUT',
        help_text='If true, the postprocessor will be invoked for RUN_TIMEOUT jobs'
        ' and it is up to the script to handle timeout and update job state.',
        default=False)
    auto_timeout_retry = models.BooleanField(
        'Automatically restart jobs that have timed out',
        help_text="If True and post_timeout_handler is False, then jobs will "
        "simply be marked RESTART_READY upon timing out.",
        default=True)

    state = models.TextField(
        'Job State',
        help_text='The current state of the job.',
        default='CREATED',
        validators=[validate_state])
    state_history = models.TextField(
        'Job State History',
        help_text="Chronological record of the job's states",
        default=history_line)

    queued_launch = models.ForeignKey(
        'QueuedLaunch',
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
    )


    @staticmethod
    def from_dict(d):
        job = BalsamJob()
        SERIAL_FIELDS = [f for f in job.__dict__ if f not in
                '_state force_insert force_update using update_fields'.split()
                ]

        if type(d['job_id']) is str:
            d['job_id'] = uuid.UUID(d['job_id'])
        else:
            assert d['job_id'] is None
            d['job_id'] = job.job_id

        for field in SERIAL_FIELDS:
            job.__dict__[field] = d[field]

        assert type(job.job_id) == uuid.UUID
        return job


    def __repr__(self):
        result = f'BalsamJob {self.pk}\n'
        result += '----------------------------------------------\n'
        result += '\n'.join( (k+':').ljust(32) + str(v) 
                for k,v in self.__dict__.items() 
                if k not in ['state_history', 'job_id', '_state', 'lock', 'tick'])
        result += '\n' + '  *** Executed command:'.ljust(32) + self.app_cmd
        result += '\n' + '  *** Working directory:'.ljust(32) + self.working_directory +'\n'
        return result

    def __str__(self):
        return self.__repr__()

    def get_parents_by_id(self):
        return json.loads(self.parents)

    def get_parents(self):
        parent_ids = self.get_parents_by_id()
        return BalsamJob.objects.filter(job_id__in=parent_ids)

    @property
    def num_ranks(self):
        return self.num_nodes * self.ranks_per_node

    @property
    def cute_id(self):
        if self.name:
            return f"[{self.name} | { str(self.pk)[:8] }]"
        else:
            return f"[{ str(self.pk)[:8] }]"
    
    @property
    def app_cmd(self):
        app = self.get_application()
        line = f"{app.executable} {self.application_args}"
        return ' '.join(os.path.expanduser(w) for w in line.split())

    def get_children(self):
        return BalsamJob.objects.filter(parents__icontains=str(self.pk))

    def get_children_by_id(self):
        children = self.get_children()
        return [c.pk for c in children]

    def get_child_by_name(self, name):
        children = self.get_children().filter(name=name)
        if children.count() == 0:
            raise ValueError(f"No child named {name}")
        elif children.count() > 1:
            raise ValueError(f"More than one child named {name}")
        else:
            return children.first()

    def set_parents(self, parents):
        try:
            parents_list = list(parents)
        except:
            raise InvalidParentsError("Cannot convert input to list")
        for i, parent in enumerate(parents_list):
            pk = parent.pk if isinstance(parent,BalsamJob) else parent
            if not BalsamJob.objects.filter(pk=pk).exists():
                raise InvalidParentsError(f"Job PK {pk} is not in the BalsamJob DB")
            parents_list[i] = str(pk)
        self.parents = json.dumps(parents_list)
        self.save(update_fields=['parents'])

    def get_application(self):
        if not self.application: 
            raise NoApplication
        elif self.application in _app_cache:
            return _app_cache[self.application]
        else:
            app = ApplicationDefinition.objects.get(name=self.application)
            _app_cache[self.application] = app
            return app

    @property
    def preprocess(self):
        try:
            app = self.get_application()
            return app.preprocess
        except NoApplication:
            return ''
    
    @property
    def postprocess(self):
        try:
            app = self.get_application()
            return app.postprocess
        except NoApplication:
            return ''

    @staticmethod
    def parse_envstring(s):
        result = {}
        entries = s.split(':')
        entries = [e.split('=') for e in entries]
        return {variable:value for (variable,value) in entries}

    def get_envs(self, *, timeout=False, error=False):
        keywords = 'BALSAM DJANGO PYTHON'.split()
        envs = {var:value for var,value in os.environ.items() 
                if any(keyword in var for keyword in keywords)}
        
        if self.environ_vars:
            job_vars = self.parse_envstring(self.environ_vars)
            envs.update(job_vars)
    
        balsam_envs = dict(
            BALSAM_JOB_ID=str(self.pk),
            BALSAM_PARENT_IDS=str(self.parents),
        )

        if self.threads_per_rank > 1:
            balsam_envs['OMP_NUM_THREADS'] = str(self.threads_per_rank)

        if timeout: balsam_envs['BALSAM_JOB_TIMEOUT']="TRUE"
        if error: balsam_envs['BALSAM_JOB_ERROR']="TRUE"
        envs.update(balsam_envs)
        return envs

    @classmethod
    @transaction.atomic
    def batch_update_state(cls, pk_list, new_state, message=''):
        try:
            exists = pk_list.exists()
        except AttributeError:
            exists = bool(pk_list)
        if not exists: return

        if new_state not in STATES:
            raise InvalidStateError(f"{new_state} is not a job state in balsam.models")

        update_jobs = cls.objects.filter(job_id__in=pk_list).select_for_update()
        update_jobs = update_jobs.exclude(state='USER_KILLED')

        #states = cls.objects.filter(job_id__in=pk_list).values_list('job_id', 'state')
        #assert len(states) == len(pk_list)
        #update_ids = [jid for (jid,state) in states if state != 'USER_KILLED']

        #update_jobs = cls.objects.filter(job_id__in=update_ids)
        msg = history_line(new_state, message)

        update_jobs.update(state=new_state,
                           state_history=Concat('state_history', V(msg))
                          )

    def update_state(self, new_state, message=''):
        if new_state not in STATES:
            raise InvalidStateError(f"{new_state} is not a job state in balsam.models")
        msg = history_line(new_state, message)
        self.state = new_state
        self.state_history += msg
        self.save(update_fields=['state', 'state_history'])

    @classmethod
    @transaction.atomic
    def release_all_locks(cls):
        running_jobs = cls.objects.all().select_for_update().filter(state='RUNNING')
        msg = history_line('RUN_TIMEOUT', 'reverted')
        running_jobs.update(state='RUN_TIMEOUT',
                            state_history=Concat('state_history', V(msg)))
        cls.objects.all().select_for_update().update(lock='')



    def get_recent_state_str(self):
        return self.state_history.split("\n")[-1].strip()

    def read_file_in_workdir(self, fname):
        work_dir = self.working_directory
        path = os.path.join(work_dir, fname)
        if not os.path.exists(path):
            raise ValueError(f"{fname} not found in working directory of {self.cute_id}")
        else:
            return open(path).read()

    def get_state_times(self):
        matches = STATE_TIME_PATTERN.findall(self.state_history)
        return {state: datetime.strptime(timestr, TIME_FMT)
                for timestr, state in matches
               }

    @property
    def runtime_seconds(self):
        times = self.get_state_times()
        t0 = times.get('RUNNING', None) 
        t1 = times.get('RUN_DONE', None) 
        if t0 and t1:
            return (t1-t0).total_seconds()
        else:
            return None

    @property
    def working_directory(self):
        top = settings.BALSAM_WORK_DIRECTORY
        if self.workflow:
            top = os.path.join(top, self.workflow)
        name = self.name.strip().replace(' ', '_')
        name += '_' + str(self.pk)[:8]
        path = os.path.join(top, name)
        return path

    def to_dict(self):
        SERIAL_FIELDS = [f for f in self.__dict__ if f not in ['_state']]
        d = {field : self.__dict__[field] for field in SERIAL_FIELDS}
        return d

    def serialize(self, **kwargs):
        d = self.to_dict()
        d.update(kwargs)
        if type(self.job_id) == uuid.UUID:
            d['job_id'] = str(self.job_id)
        else:
            assert self.job_id == d['job_id'] == None

        serial_data = json.dumps(d)
        return serial_data

    @classmethod
    def deserialize(cls, serial_data):
        if type(serial_data) is bytes:
            serial_data = serial_data.decode('utf-8')
        if type(serial_data) is str:
            serial_data = json.loads(serial_data)
        job = BalsamJob.from_dict(serial_data)
        return job

class ApplicationDefinition(models.Model):
    ''' application definition, each DB entry is a task that can be run
        on the local resource. '''
    name = models.TextField(
        'Application Name',
        help_text='The name of an application that can be run locally.',
        default='')
    description = models.TextField(
        'Application Description',
        help_text='A description of the application.',
        default='')
    executable = models.TextField(
        'Executable',
        help_text='The executable path to run this application on the local system.',
        default='')
    preprocess = models.TextField(
        'Preprocessing Script',
        help_text='A script that is run in a job working directory prior to submitting the job to the queue.',
        default='')
    postprocess = models.TextField(
        'Postprocessing Script',
        help_text='A script that is run in a job working directory after the job has completed.',
        default='')

    def __repr__(self):
        return f'''
Application {self.pk}:
-----------------------------
Name:           {self.name}
Description:    {self.description}
Executable:     {self.executable}
Preprocess:     {self.preprocess}
Postprocess:    {self.postprocess}
'''.strip() + '\n'
    
    def __str__(self):
        return self.__repr__()

    @property
    def cute_id(self):
        return f"[{self.name} | { str(self.pk)[:8] }]"

