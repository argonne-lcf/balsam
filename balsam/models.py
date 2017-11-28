import os
import json
import logging
import sys
from datetime import datetime
from socket import gethostname
import uuid

from django.core.exceptions import ValidationError
from django.conf import settings
from django.db import models
from concurrency.fields import IntegerVersionField

from common import Serializer
from balsam import scheduler, BalsamJobMessage

logger = logging.getLogger(__name__)

class InvalidStateError(ValidationError): pass
class InvalidParentsError(ValidationError): pass

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
    newline = '' if state=='CREATED' else '\n'
    return newline + f"[{get_time_string()} {state}] ".rjust(46) + message


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
        help_text="A string of filename patterns that will be searched in the parents'"\
        "working directories. Every matching file will be made available in this"\
        "job's working directory (symlinks for local Balsam jobs, file transfer for"\
        "remote Balsam jobs). Default: all files from parent jobs are made available.",
        default='*')
    stage_in_urls = models.TextField(
        'External stage in files or folders', help_text="A list of URLs for external data to be staged in prior to job processing. Job dataflow from parents to children is NOT handled here; see `input_files` field instead.",
        default='')
    stage_out_files = models.TextField(
        'External stage out files or folders',
        help_text="A string of filename patterns. Matches will be transferred to the stage_out_url. Default: no files are staged out",
        default='')
    stage_out_urls = models.TextField(
        'Stage Out URL',
        help_text='The URLs to which designated stage out files are sent.',
        default='')

    requested_wall_time_minutes = models.IntegerField(
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
    processes_per_node = models.IntegerField(
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
    
    ping_info = models.TextField(
        'Scheduler ID',
        help_text='Information on the service (such as scheduler ID, queue) that most recently touched this job',
        default='{}')

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
        s = f'''
BalsamJob:              {self.job_id}
state:                  {self.state}
work_site:              {self.work_site}
workflow:               {self.workflow}
name:                   {self.name}
description:            {self.description[:50]}
working_directory:      {self.working_directory}
parents:                {self.parents}
input_files:            {self.input_files}
stage_in_urls:          {self.stage_in_urls}
stage_out_files:        {self.stage_out_files}
stage_out_urls:         {self.stage_out_urls}
wall_time_minutes:      {self.wall_time_minutes}
num_nodes:              {self.num_nodes}
processes_per_node:     {self.processes_per_node}
ping_info:              {self.ping_info}
runtime_seconds:        {self.runtime_seconds}
application:            {self.application}
'''
        return s.strip() + '\n'
    
    def idle(self):
        '''job.ping_info has a 'ping' time key: 1) if key missing, job has not
        been touched yet 2) if None, then service has signalled job is now free.
        If the job is LAUNCHER_QUEUED and appears in local scheduler, it's busy.
        Otherwise, the job is idle if it has not been pinged in the last 5
        minutes (signalling that a service processing the job crashed)'''
        info = self.get_ping_info()
        if 'ping' not in info: return True
        if info['ping'] is None: return True # signals idle

        sched_id = info['scheduler_id']
        if self.state == 'LAUNCHER_QUEUED' and sched_id:
            try: queue_stat = scheduler.get_job_status(sched_id)
            except scheduler.NoQStatInformation: return True # not in queue
            else: return False # in queue; not idle

        last_ping = (info['ping'] - datetime.now()).total_seconds()
        if last_ping > 300.0: return True # detect hard failure; dead launcher
        else: return False
    
    def get_ping_info(self):
        info = json.loads(self.ping_info)
        if info['ping'] is not None:
            info['ping'] = from_time_string(info['ping'])
        return info

    def service_ping(self, *, scheduler_id=None, set_idle=False):
        if set_idle: time = None
        else: time = get_time_string()

        pid = os.getpid()
        hostname = gethostname()
        info = dict(ping=time, scheduler_id=scheduler_id, pid=pid,
                    hostname=hostname)
        self.ping_info = json.dumps(info)
        self.save(update_fields=['ping_info'])

    def get_parents_by_id(self):
        return json.loads(self.parents)

    def get_parents(self):
        parent_ids = self.get_parents_by_id()
        return BalsamJob.objects.filter(job_id__in=parent_ids)

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

    def update_state(self, new_state, message=''):
        if new_state not in STATES:
            raise InvalidStateError(f"{new_state} is not a job state in balsam.models")

        self.state_history += history_line(new_state, message)
        self.state = new_state
        self.save(update_fields=['state', 'state_history'])

    def get_line_string(self):
        recent_state = self.state_history.split("\n")[-1]
        return f' {str(self.job_id):36} | {self.workflow:26} | {self.name:26} | {self.application:26} | {self.work_site:20} | {recent_state:100}'

    @staticmethod
    def get_header():
        return f' {"job_id":36} | {"workflow":26} | {"name":26} | {"application":26} | {"work_site":20} | {"recent state":100}'

    def create_working_path(self):
        top = settings.BALSAM_WORK_DIRECTORY
        if self.workflow:
            top = os.path.join(top, self.workflow)
        name = self.name.replace(' ', '_')
        path = os.path.join(top, name)
        for char in str(self.job_id):
            if not os.path.exists(path): break
            path += char
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

    def __str__(self):
        s = 'Application: ' + self.name + '\n'
        s += '  description:   ' + self.description + '\n'
        s += '  executable:    ' + self.executable + '\n'
        s += '  config_script: ' + self.config_script + '\n'
        s += '  preprocess:    ' + self.preprocess + '\n'
        s += '  postprocess:   ' + self.postprocess + '\n'
        return s

    def get_line_string(self):
        format = ' %7i | %20s | %20s | %20s | %20s | %20s | %s '
        output = format % (self.pk, self.name, self.executable, self.config_script,
                           self.preprocess, self.postprocess,
                           self.description)
        return output

    @staticmethod
    def get_header():
        format = ' %7s | %20s | %20s | %20s | %20s | %20s | %s '
        output = format % ('pk', 'name', 'executable', 'config_script',
                           'preprocess', 'postprocess',
                           'description')
        return output
