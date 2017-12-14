import os
import json
import logging
import sys
from datetime import datetime
from socket import gethostname
import uuid

from django.core.exceptions import ValidationError,ObjectDoesNotExist
from django.conf import settings
from django.db import models
from concurrency.fields import IntegerVersionField

from common import Serializer

logger = logging.getLogger(__name__)

class InvalidStateError(ValidationError): pass
class InvalidParentsError(ValidationError): pass
class NoApplication(Exception): pass

TIME_FMT = '%m-%d-%Y %H:%M:%S'

STATES = '''
CREATED
LAUNCHER_QUEUED
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
PARENT_KILLED'''.split()

ACTIVE_STATES = '''
RUNNING
'''.split()

PROCESSABLE_STATES = '''
CREATED
LAUNCHER_QUEUED
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
PARENT_KILLED'''.split()

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
    return datetime.now().strftime(TIME_FMT)

def from_time_string(s):
    return datetime.strptime(s, TIME_FMT)

def history_line(state='CREATED', message=''):
    return f"\n[{get_time_string()} {state}] ".rjust(46) + message


class BalsamJob(models.Model):
    ''' A DB representation of a Balsam Job '''

    version = IntegerVersionField() # optimistic lock

    job_id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False)
    allowed_work_sites = models.TextField(
        'Allowed Work Sites',
        help_text='Name of the Balsam instance(s) where this job can run.',
        default='')
    work_site = models.TextField(
        'Actual work site',
        help_text='Name of the Balsam instance that handled this job.',
        default='')

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

    working_directory = models.TextField(
        'Local Job Directory',
        help_text='Local working directory where job files are stored.',
        default='')
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
    runtime_seconds = models.FloatField(
        'Measured Job Execution Time in seconds',
        help_text='The actual elapsed runtime of the job, measured by launcher.',
        default=0.0)
    num_nodes = models.IntegerField(
        'Number of Compute Nodes',
        help_text='The number of compute nodes requested for this job.',
        default=1)
    ranks_per_node = models.IntegerField(
        'Number of Processes per Node',
        help_text='The number of MPI processes per node to schedule for this job.',
        default=1)
    threads_per_rank = models.IntegerField(
        'Number of threads per MPI rank',
        help_text='The number of OpenMP threads per MPI rank (if applicable)',
        default=1)
    threads_per_core = models.IntegerField(
        'Number of hyperthreads per physical core (if applicable)',
        help_text='Number of hyperthreads per physical core.',
        default=1)
    environ_vars = models.TextField(
        'Environment variables specific to this job',
        help_text="Colon-separated list of envs like VAR1=value1:VAR2=value2",
        default='')
    
    scheduler_id = models.TextField(
        'Scheduler ID',
        help_text='Scheduler ID (if job assigned by metascheduler)',
        default='')

    application = models.TextField(
        'Application to Run',
        help_text='The application to run; located in Applications database',
        default='')
    application_args = models.TextField(
        'Command-line args to the application exe',
        help_text='Command line arguments used by the Balsam job runner',
        default='')

    direct_command = models.TextField(
        'Command line to execute (specified with balsam qsub <args> <command>)',
        help_text="Instead of creating BalsamJobs that point to a pre-defined "
        "application, users can directly add jobs consisting of a single command "
        "line with `balsam qsub`.  This direct command is then invoked by the  "
        "Balsam job launcher.",
        default='')

    preprocess = models.TextField(
        'Preprocessing Script',
        help_text='A script that is run in a job working directory prior to submitting the job to the queue.'
        ' If blank, will default to the default_preprocess script defined for the application.',
        default='')
    postprocess = models.TextField(
        'Postprocessing Script',
        help_text='A script that is run in a job working directory after the job has completed.'
        ' If blank, will default to the default_postprocess script defined for the application.',
        default='')
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
    
    def save(self, force_insert=False, force_update=False, using=None, 
             update_fields=None):
        '''Override default Django save to ensure version always updated'''
        if update_fields is not None: 
            update_fields.append('version')
        if self._state.adding:
            update_fields = None
        models.Model.save(self, force_insert, force_update, using, update_fields)

    def __str__(self):
        return f'''
Balsam Job
----------
ID:                     {self.pk}
name:                   {self.name} 
workflow:               {self.workflow}
latest state:           {self.get_recent_state_str()}
description:            {self.description[:80]}
work site:              {self.work_site} 
allowed work sites:     {self.allowed_work_sites}
working_directory:      {self.working_directory}
parents:                {self.parents}
input_files:            {self.input_files}
stage_in_url:           {self.stage_in_url}
stage_out_url:          {self.stage_out_url}
stage_out_files:        {self.stage_out_files}
wall_time_minutes:      {self.wall_time_minutes}
actual_runtime:         {self.runtime_str()}
num_nodes:              {self.num_nodes}
threads per rank:       {self.threads_per_rank}
threads per core:       {self.threads_per_core}
ranks_per_node:         {self.ranks_per_node}
scheduler_id:           {self.scheduler_id}
application:            {self.application if self.application else 
                            self.direct_command}
args:                   {self.application_args}
envs:                   {self.environ_vars}
created with qsub:      {bool(self.direct_command)}
preprocess override:    {self.preprocess}
postprocess override:   {self.postprocess}
post handles error:     {self.post_error_handler}
post handles timeout:   {self.post_timeout_handler}
auto timeout retry:     {self.auto_timeout_retry}
'''.strip() + '\n'
    

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
        if self.application:
            app = ApplicationDefinition.objects.get(name=self.application)
            line = f"{app.executable} {self.application_args}"
        else:
            line = self.direct_command
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
                raise InvalidParentsError("Job PK {pk} is not in the BalsamJob DB")
            parents_list[i] = str(pk)
        self.parents = json.dumps(parents_list)
        self.save(update_fields=['parents'])

    def get_application(self):
        if self.application:
            return ApplicationDefinition.objects.get(name=self.application)
        else:
            raise NoApplication

    @staticmethod
    def parse_envstring(s):
        result = {}
        entries = s.split(':')
        entries = [e.split('=') for e in entries]
        return {variable:value for (variable,value) in entries}

    def get_envs(self, *, timeout=False, error=False):
        keywords = 'PATH LIBRARY BALSAM DJANGO PYTHON'.split()
        envs = {var:value for var,value in os.environ.items() 
                if any(keyword in var for keyword in keywords)}
        try:
            app = self.get_application()
        except NoApplication:
            app = None
        if app and app.environ_vars:
            app_vars = self.parse_envstring(app.environ_vars)
            envs.update(app_vars)
        if self.environ_vars:
            job_vars = self.parse_envstring(self.environ_vars)
            envs.update(job_vars)
    
        children = self.get_children_by_id()
        children = json.dumps([str(c) for c in children])
        balsam_envs = dict(
            BALSAM_JOB_ID=str(self.pk),
            BALSAM_PARENT_IDS=str(self.parents),
            BALSAM_CHILD_IDS=children,
        )
        if timeout: balsam_envs['BALSAM_JOB_TIMEOUT']="TRUE"
        if error: balsam_envs['BALSAM_JOB_ERROR']="TRUE"
        envs.update(balsam_envs)
        return envs

    def update_state(self, new_state, message='',using=None):
        if new_state not in STATES:
            raise InvalidStateError(f"{new_state} is not a job state in balsam.models")

        self.state_history += history_line(new_state, message)
        self.state = new_state
        self.save(update_fields=['state', 'state_history'],using=using)

    def get_recent_state_str(self):
        return self.state_history.split("\n")[-1].strip()

    def read_file_in_workdir(self, fname):
        work_dir = self.working_directory
        path = os.path.join(work_dir, fname)
        if not os.path.exists(path):
            raise ValueError(f"{fname} not found in working directory of"
            " {self.cute_id}")
        else:
            return open(path).read()

    def get_line_string(self):
        recent_state = self.get_recent_state_str()
        app = self.application if self.application else self.direct_command
        return f' {str(self.pk):36} | {self.name:26} | {self.workflow:26} | {app:26} | {recent_state}'

    def runtime_str(self):
        if self.runtime_seconds == 0: return ''
        minutes, seconds = divmod(self.runtime_seconds, 60)
        hours, minutes = divmod(minutes, 60)
        if hours: return f"{hours:02d} hr : {minutes:02d} min : {seconds:02d} sec"
        else: return f"{minutes:02d} min : {seconds:02d} sec"

    @staticmethod
    def get_header():
        return f' {"job_id":36} | {"name":26} | {"workflow":26} | {"application":26} | {"latest update"}'

    def create_working_path(self):
        top = settings.BALSAM_WORK_DIRECTORY
        if self.workflow:
            top = os.path.join(top, self.workflow)
        name = self.name.strip().replace(' ', '_')
        name += '_' + str(self.pk)[:8]
        path = os.path.join(top, name)

        if os.path.exists(path):
            jid = str(self.pk)[8:]
            path += jid[0]
            i = 1
            while os.path.exists(path):
                path += jid[i]
                i += 1
                
        os.makedirs(path)
        self.working_directory = path
        self.save(update_fields=['working_directory'])
        return path

    def serialize(self):
        pass

    @classmethod
    def deserialize(cls, serial_data):
        pass


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
        help_text='The executable and path need to run this application on the local system.',
        default='')
    default_preprocess = models.TextField(
        'Preprocessing Script',
        help_text='A script that is run in a job working directory prior to submitting the job to the queue.',
        default='')
    default_postprocess = models.TextField(
        'Postprocessing Script',
        help_text='A script that is run in a job working directory after the job has completed.',
        default='')
    environ_vars = models.TextField(
        'Environment variables specific to this application',
        help_text="Colon-separated list of envs like VAR1=value2:VAR2=value2",
        default='')

    def __str__(self):
        return f'''
Application:
------------
PK:             {self.pk}
Name:           {self.name}
Description:    {self.description}
Executable:     {self.executable}
Preprocess:     {self.default_preprocess}
Postprocess:    {self.default_postprocess}
Envs:           {self.environ_vars}
'''.strip() + '\n'

    def get_line_string(self):
        format = ' %20s | %20s | %20s | %20s | %s '
        output = format % (self.name, self.executable,
                           self.default_preprocess, 
                           self.default_postprocess,
                           self.description)
        return output

    @staticmethod
    def get_header():
        format = ' %20s | %20s | %20s | %20s | %s '
        output = format % ('name', 'executable',
                           'preprocess', 'postprocess',
                           'description')
        return output
    
    @property
    def cute_id(self):
        return f"[{self.name} | { str(self.pk)[:8] }]"
