

# ------------- BalsamJob Transitions -------------------

import multiprocessing,os,time,logging,sys,datetime
logger = logging.getLogger(__name__)

from django.db import utils,connections,DEFAULT_DB_ALIAS
from django.core.exceptions import ObjectDoesNotExist
from django.conf import settings

from balsam import BalsamJobStatus
from common import transfer,MessageInterface,run_subprocess
from common import log_uncaught_exceptions,db_tools,Serializer
from balsam import scheduler,BalsamJobMessage
from balsam.schedulers import exceptions

# assign this function to the system exception hook
sys.excepthook = log_uncaught_exceptions.log_uncaught_exceptions

# stage in files for a job
def stage_in(job):
   ''' if the job an input_url defined,
       the files are copied to the local working_directory '''
   logger.debug('in stage_in')
   message = 'job staged in'
   if job.input_url != '':
      try:
         transfer.stage_in( job.input_url + '/', job.working_directory + '/' )
         job.state = STAGED_IN.name
      except Exception as e:
         message = 'Exception received during stage_in: ' + str(e)
         logger.error(message)
         job.state = STAGE_IN_FAILED.name
   else:
      # no input url specified so stage in is complete
      job.state = STAGED_IN.name

   job.save(update_fields=['state'],using=db_tools.get_db_connection_id(job.pk))
   send_status_message(job,message)

# stage out files for a job
def stage_out(job):
   ''' if the job has files defined via the output_files and an output_url is defined,
       they are copied from the local working_directory to the output_url '''
   logger.debug('in stage_out')
   message = None
   if job.output_url != '':
      try:
         transfer.stage_out( self.working_directory + '/', self.output_url + '/' )
         job.state = STAGED_OUT.name
      except Exception as e:
         message = 'Exception received during stage_out: ' + str(e)
         logger.error(message)
         job.state = STAGE_OUT_FAILED.name
   else:
      # no output url specififed so stage out is complete
      job.state = STAGED_OUT.name

   job.save(update_fields=['state'],using=db_tools.get_db_connection_id(job.pk))
   send_status_message(job,message)

# preprocess a job
def preprocess(job):
   ''' Each job defines a task to perform, so tasks need preprocessing to prepare
       for the job to be submitted to the batch queue. '''
   logger.debug('in preprocess ')
   message = 'Job prepocess complete.'
   # get the task that is running
   try:
      app = ApplicationDefinition.objects.get(name=job.application)
      if app.preprocess:
          if os.path.exists(app.preprocess):
             stdout = run_subprocess.run_subprocess(app.preprocess)
             # write stdout to log file
             f = open(os.path.join(job.working_directory,app.name+'.preprocess.log.pid' + str(os.getpid())),'w')
             f.write(stdout)
             f.close()
             job.state = PREPROCESSED.name
          else:
             message = ('Preprocess, "' + app.preprocess + '", of application, "' + str(job.application) 
                   + '", does not exist on filesystem.')
             logger.error(message)
             job.state = PREPROCESS_FAILED.name
      else:
         logger.debug('No preprocess specified for this job; skipping')
         job.state = PREPROCESSED.name
   except run_subprocess.SubprocessNonzeroReturnCode as e:
      message = ('Preprocess, "' + app.preprocess + '", of application, "' + str(job.application) 
               + '", exited with non-zero return code: ' + str(returncode))
      logger.error(message)
      job.state = PREPROCESS_FAILED.name
   except run_subprocess.SubprocessFailed as e:
      message = ('Received exception while running preprocess, "' + app.preprocess 
               + '", of application, "' + str(job.application) + '", exception: ' + str(e))
      logger.error(message)
      job.state = PREPROCESS_FAILED.name
   except ObjectDoesNotExist as e:
      message = 'application,' + str(job.application) + ', does not exist.'
      logger.error(message)
      job.state = PREPROCESS_FAILED.name
   except Exception as e:
      message = 'Received exception while in preprocess, "' + app.preprocess + '", for application ' + str(job.application)
      logger.exception(message)
      job.state = PREPROCESS_FAILED.name
   
   job.save(update_fields=['state'],using=db_tools.get_db_connection_id(job.pk))
   send_status_message(job,message)

# submit the job to the local scheduler
def submit(job):
   ''' this function submits the job to the local batch system '''
   logger.debug('in submit')
   message = ''
   try:
      # some schedulers have limits on the number of jobs that can
      # be queued, so check to see if we are at that number
      # If so, don't submit the job
      jobs_queued = BalsamJob.objects.filter(state=QUEUED.name)
      if len(jobs_queued) <= settings.BALSAM_MAX_QUEUED:
         cmd = job.get_application_command()
         scheduler.submit(job,cmd)
         job.state = SUBMITTED.name
         message = 'Job entered SUBMITTED state'
      else:
         message = 'Job submission delayed due to local queue limits'
   except exceptions.SubmitNonZeroReturnCode as e:
      message = 'scheduler returned non-zero value during submit command: ' + str(e)
      logger.error(message)
      job.state = SUBMIT_FAILED.name
   except exceptions.SubmitSubprocessFailed as e:
      message = 'subprocess in scheduler submit failed: ' + str(e)
      logger.error(message)
      job.state = SUBMIT_FAILED.name
   except exceptions.JobSubmissionDisabled as e:
      message = 'scheduler job submission is currently disabled: ' + str(e)
      logger.error(message)
      job.state = SUBMIT_DISABLED.name
   except Exception as e:
      message = 'received exception while calling scheduler submit for job ' + str(job.job_id) + ', exception: ' + str(e)
      logger.exception(message)
      job.state = SUBMIT_FAILED.name
   
   job.save(update_fields=['state','scheduler_id'],using=db_tools.get_db_connection_id(job.pk))
   logger.debug('sending status message')
   send_status_message(job,message)
   logger.debug('submit done')

   

# perform any post job processing needed
def postprocess(job):
   ''' some jobs need to have some postprocessing performed,
       this function does this.'''
   logger.debug('in postprocess ' )
   message = 'Job postprocess complete'
   try:
      app = ApplicationDefinition.objects.get(name=job.application)
      if app.postprocess:
          if os.path.exists(app.postprocess):
             stdout = run_subprocess.run_subprocess(app.postprocess)
             # write stdout to log file
             f = open(os.path.join(job.working_directory,app.name+'.postprocess.log.pid' + str(os.getpid())),'w')
             f.write(stdout)
             f.close()
             job.state = POSTPROCESSED.name
          else:
             message = ('Postprocess, "' + app.postprocess + '", of application, "' + str(job.application) 
                   + '", does not exist on filesystem.')
             logger.error(message)
             job.state = POSTPROCESS_FAILED.name
      else:
         logger.debug('No postprocess specified for this job; skipping')
         job.state = POSTPROCESSED.name
   except run_subprocess.SubprocessNonzeroReturnCode as e:
      message = ('Postprocess, "' + app.postprocess + '", of application, "' + str(job.application) 
               + '", exited with non-zero return code: ' + str(returncode))
      logger.error(message)
      job.state = POSTPROCESS_FAILED.name
   except run_subprocess.SubprocessFailed as e:
      message = ('Received exception while running postprocess, "' + app.preprocess 
               + '", of application, "' + str(job.application) + '", exception: ' + str(e))
      logger.error(message)
      job.state = POSTPROCESS_FAILED.name
   except ObjectDoesNotExist as e:
      message = 'application,' + str(job.application) + ', does not exist.'
      logger.error(message)
      job.state = POSTPROCESS_FAILED.name
   except Exception as e:
      message = 'Received exception while in postprocess, "' + app.postprocess + '", for application ' + str(job.application)
      logger.error(message)
      job.state = POSTPROCESS_FAILED.name
   
   job.save(update_fields=['state'],using=db_tools.get_db_connection_id(job.pk))
   send_status_message(job,message)
   
def finish_job(job):
   ''' simply change state to Finished and send status to user '''
   job.state = JOB_FINISHED.name
   job.save(update_fields=['state'],using=db_tools.get_db_connection_id(job.pk))
   send_status_message(job,'Job finished')


def send_status_message(job,message=''):
   ''' send a status message describing a job state '''
   return
   logger.debug('in send_status_message')
   # setup message interface
   try:
      p = MessageInterface.MessageInterface(
                          host           =  settings.RABBITMQ_SERVER_NAME,
                          port           =  settings.RABBITMQ_SERVER_PORT,
                          exchange_name  =  settings.RABBITMQ_BALSAM_EXCHANGE_NAME,
                          ssl_cert       =  settings.RABBITMQ_SSL_CERT,
                          ssl_key        =  settings.RABBITMQ_SSL_KEY,
                          ssl_ca_certs   =  settings.RABBITMQ_SSL_CA_CERTS,
                         )
      p.open_blocking_connection()
   
      statmsg = BalsamJobStatus.BalsamJobStatus(job,message)
      p.send_msg(statmsg.serialize(), settings.RABBITMQ_BALSAM_JOB_STATUS_ROUTING_KEY)
      p.close()
   except Exception as e:
      logger.exception('job(pk='+str(job.pk)+',id='+str(job.job_id)+
           '): Failed to send BalsamJobStatus message, received exception')

# -------- Job States ------------------------

from common.JobState import JobState

CREATE_FAILED        = JobState('CREATE_FAILED')
CREATED              = JobState('CREATED',CREATE_FAILED,stage_in)
STAGE_IN_FAILED      = JobState('STAGE_IN_FAILED')
STAGED_IN            = JobState('STAGED_IN',STAGE_IN_FAILED,preprocess)
PREPROCESS_FAILED    = JobState('PREPROCESS_FAILED')
PREPROCESSED         = JobState('PREPROCESSED',PREPROCESS_FAILED,submit)
SUBMIT_FAILED        = JobState('SUBMIT_FAILED')
SUBMITTED            = JobState('SUBMITTED',SUBMIT_FAILED)
SUBMIT_DISABLED      = JobState('SUBMIT_DISABLED',SUBMIT_FAILED,postprocess)

QUEUED               = JobState('QUEUED')
RUNNING              = JobState('RUNNING')
EXECUTION_FAILED     = JobState('EXECUTION_FAILED',None,postprocess)
EXECUTION_FINISHED   = JobState('EXECUTION_FINISHED',EXECUTION_FAILED,postprocess)

POSTPROCESS_FAILED   = JobState('POSTPROCESS_FAILED')
POSTPROCESSED        = JobState('POSTPROCESSED',POSTPROCESS_FAILED,stage_out)
STAGE_OUT_FAILED     = JobState('STAGE_OUT_FAILED')
STAGED_OUT           = JobState('STAGED_OUT',STAGE_OUT_FAILED,finish_job)

JOB_FAILED           = JobState('JOB_FAILED')
JOB_FINISHED         = JobState('JOB_FINISHED',JOB_FAILED)
JOB_REJECTED         = JobState('JOB_REJECTED')

STATES = [
   CREATED,
   CREATE_FAILED,
   STAGED_IN,
   STAGE_IN_FAILED,
   PREPROCESSED,
   PREPROCESS_FAILED,
   SUBMITTED,
   SUBMIT_FAILED,
   SUBMIT_DISABLED,

   QUEUED,
   RUNNING,
   EXECUTION_FINISHED,
   EXECUTION_FAILED,

   POSTPROCESSED,
   POSTPROCESS_FAILED,
   STAGED_OUT,
   STAGE_OUT_FAILED,

   JOB_FINISHED,
   JOB_FAILED,
   JOB_REJECTED,
]

TRANSITIONABLE_STATES = []
for state in STATES:
   if state.transition_function is not None:
      TRANSITIONABLE_STATES.append(state.name)


CHECK_STATUS_STATES = [
   SUBMITTED.name,
   QUEUED.name,
   RUNNING.name,
]

STATES_BY_NAME = { x.name:x for x in STATES }


#----------------- BalsamJob Definition ----------------

from django.db import models

class BalsamJob(models.Model):
   ''' A DB representation of a Balsam Job '''

   # a unique job id
   job_id                        = models.BigIntegerField('Job ID',help_text='A unique id for this job.',default=0)
   site                          = models.TextField('Site Name',help_text='The name of the computer system, supercomputer, or location where Balsam is running.',default='')

   # an arbitrary name, this is here for the user
   name                          = models.TextField('Job Name',help_text='A name for the job given by the user.',default='')
   description                   = models.TextField('Job Description',help_text='A description of the job.',default='')
   argo_job_id                   = models.BigIntegerField('Origin ID',help_text='The ID of the Argo job to which this subjob belongs. Can be set to 0 if there is no Argo job set.',default=0)
   
   # scheduler specific attributes
   queue                         = models.TextField('Scheduler Queue',help_text='The local scheduler queue to which to submit jobs.',default=settings.BALSAM_DEFAULT_QUEUE)
   project                       = models.TextField('Scheduler Project Name',help_text='The local scheduler project with which to submit jobs.',default=settings.BALSAM_DEFAULT_PROJECT)
   wall_time_minutes             = models.IntegerField('Job Wall Time in Minutes',help_text='The number of minutes the job is expected to take and after which the scheduler will kill the job.',default=0)
   num_nodes                     = models.IntegerField('Number of Compute Nodes',help_text='The number of compute nodes to schedule for this job.',default=0)
   processes_per_node            = models.IntegerField('Number of Processes per Node',help_text='The number of processes to node to schedule for this job.',default=0)
   scheduler_config              = models.TextField('Scheduler Options',help_text='options to pass to the scheduler',default='')
   scheduler_id                  = models.IntegerField('Scheduler ID',help_text='The ID assigned this job after being submitted to the queue.',default=0)
   
   # task attributes
   # task_id specifies the task to run
   application                   = models.TextField('Application to Run',help_text='This is the name of an application that lives in the database as an ApplicationDefinition.',default='')
   config_file                   = models.TextField('Configuration File',help_text='This is the input file provided by the users which is used to configure the application. This may be options that are typically included on the command line. It depends on the applications.',default='')

   state                         = models.TextField('Job State',help_text='The current state of the job.',default=CREATED.name)
   working_directory             = models.TextField('Local Job Directory',help_text='Local working directory where job files are stored and usually the job is running here.',default='')

   input_url                     = models.TextField('Input URL',help_text='The URL from which to retrieve input data.',default='')
   output_url                    = models.TextField('Output URL',help_text='The URL to which to place output data.',default='')
   
   time_created                  = models.DateTimeField('Creation Time',help_text='The time at which the job was created in the database.',auto_now_add=True)
   time_modified                 = models.DateTimeField('Modified Time',help_text='The last time the job was modified in the database.',auto_now=True)
   time_start_queued             = models.DateTimeField('Queue Entry Time',help_text='The time when the job was added to the queue.',null=True)
   time_job_started              = models.DateTimeField('Job Run Start Time',help_text='The time when the job started running.',null=True)
   time_job_finished             = models.DateTimeField('Job Finish Time',help_text='The time at which the job stopped running.',null=True)

   SERIAL_FIELDS =   [
                        'job_id',
                        'site',
                        'name',
                        'description',
                        'argo_job_id',
                        'queue',
                        'project',
                        'wall_time_minutes',
                        'num_nodes',
                        'processes_per_node',
                        'scheduler_config',
                        'scheduler_id',
                        'application',
                        'config_file',
                        'state',
                        'working_directory',
                        'input_url',
                        'output_url',
                        'time_created',
                        'time_modified',
                        'time_job_started',
                        'time_job_finished',
                     ]
   DATETIME_FIELDS = [
                        'time_created',
                        'time_modified',
                        'time_job_started',
                        'time_job_finished',
                     ]

   def __str__(self):
      s = 'BalsamJob: '               + str(self.job_id) + '\n'
      s += '  site:                 ' + self.site + '\n'
      s += '  name:                 ' + self.name + '\n'
      s += '  description:          ' + self.description + '\n'
      s += '  argo_job_id:          ' + str(self.argo_job_id) + '\n'
      s += '  queue:                ' + self.queue + '\n'
      s += '  project:              ' + self.project + '\n'
      s += '  wall_time_minutes:    ' + str(self.wall_time_minutes) + '\n'
      s += '  num_nodes:            ' + str(self.num_nodes) + '\n'
      s += '  processes_per_node:   ' + str(self.processes_per_node) + '\n'
      s += '  scheduler_config:     ' + self.scheduler_config + '\n'
      s += '  scheduler_id:         ' + str(self.scheduler_id) + '\n'
      s += '  application:          ' + self.application + '\n'
      s += '  config_file:          ' + self.config_file + '\n'
      s += '  state:                ' + self.state + '\n'
      s += '  working_directory:    ' + self.working_directory + '\n'
      s += '  input_url:            ' + self.input_url + '\n'
      s += '  output_url:           ' + self.output_url + '\n'
      s += '  time_created:         ' + str(self.time_created) + '\n'
      s += '  time_modified:        ' + str(self.time_modified) + '\n'
      s += '  time_start_queued:    ' + str(self.time_start_queued) + '\n'
      s += '  time_job_started:     ' + str(self.time_job_started) + '\n'
      s += '  time_job_finished:    ' + str(self.time_job_finished) + '\n'
      return s

   def get_line_string(self):
      format = ' %7i | %18i | %18i | %15s | %20s | %9i | %8i | %10s | %10s | %10s | %15s '
      output = format % (self.pk,self.job_id,self.argo_job_id,self.state,self.site,
            self.num_nodes,self.processes_per_node,self.scheduler_id,self.queue,
            self.project,self.application)
      return output

   @staticmethod
   def get_header():
      format = ' %7s | %18s | %18s | %15s | %20s | %9s | %8s | %10s | %10s | %10s | %15s '
      output = format % ('pk','job_id','argo_job_id','state','site',
            'num_nodes','procs','sched_id','queue','project','application')
      return output

   def get_application_command(self):
      
      app = ApplicationDefinition.objects.get(name=self.application)
      cmd = app.executable + ' '
      if self.config_file != '':
         stdout = run_subprocess.run_subprocess(app.config_script + ' ' + self.config_file)
         cmd += stdout
      return cmd

   @staticmethod
   def generate_job_id():
      # time.time() is a double with units seconds
      # so grabing the number of microseconds
      job_id = int(time.time()*1e6)
      # make sure no jobs with the same job_id
      same_jobs = BalsamJob.objects.filter(job_id=job_id)
      while len(same_jobs) > 0:
         job_id = int(time.time()*1e6)
         same_jobs = BalsamJob.objects.filter(job_id=job_id)
      return job_id

   @staticmethod
   def create_working_path(job_id):
      path = os.path.join(settings.BALSAM_WORK_DIRECTORY,str(job_id))
      try:
         os.makedirs(path)
      except:
         logger.exception(' Received exception while making job working directory: ')
         raise
      return path

   def get_balsam_job_message(self):
      msg = BalsamJobMessage.BalsamJobMessage()
      msg.argo_job_id         = self.subjob_id
      msg.site                = self.site
      msg.name                = self.name
      msg.description         = self.description
      msg.queue               = self.queue
      msg.project             = self.project
      msg.wall_time_minutes   = self.wall_time_minutes
      msg.num_nodes           = self.num_nodes
      msg.processes_per_node  = self.processes_per_node
      msg.scheduler_config    = self.scheduler_config
      msg.application         = self.application
      msg.config_file         = self.config_file
      msg.input_url           = self.input_url
      msg.output_url          = self.output_url
      return msg

   def serialize(self):
      d = {}
      for field in BalsamJob.SERIAL_FIELDS:
         d[field] = self.__dict__[field]
      return Serializer.serialize(d)

   def deserialize(self,serial_data):
      d = Serializer.deserialize(serial_data)
      for field,value in d.items():
         if field in DATETIME_FIELDS and value != None:
            self.__dict__[field] = datetime.datetime.strptime(value,"%Y-%m-%d %H:%M:%S %z")
         else:
            self.__dict__[field] = value

   
class ApplicationDefinition(models.Model):
   ''' application definition, each DB entry is a task that can be run
       on the local resource. '''
   name                          = models.TextField('Application Name',help_text='The name of an application that can be run locally.',default='')
   description                   = models.TextField('Application Description',help_text='A description of the application.',default='')
   executable                    = models.TextField('Executable',help_text='The executable and path need to run this application on the local system.',default='')
   config_script                 = models.TextField('Configure Script',help_text='The script which digests the input configuration file and can craft the command line for the application or perform other configuration needs.',default='')
   preprocess                    = models.TextField('Preprocessing Script',help_text='A script that is run in a job working directory prior to submitting the job to the queue.',default='')
   postprocess                   = models.TextField('Postprocessing Script',help_text='A script that is run in a job working directory after the job has completed.',default='')

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
      output = format % (self.pk,self.name,self.executable,self.config_script,
            self.preprocess,self.postprocess,
            self.description)
      return output

   @staticmethod
   def get_header():
      format = ' %7s | %20s | %20s | %20s | %20s | %20s | %s '
      output = format % ('pk','name','executable','config_script',
            'preprocess','postprocess',
            'description')
      return output






