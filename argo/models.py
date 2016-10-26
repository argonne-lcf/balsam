
#-----------  ArgoJob Transitions ---------------

import multiprocessing,sys,logging
logger = logging.getLogger(__name__)

from django.db import utils,connections,DEFAULT_DB_ALIAS,models
from django.core.exceptions import ObjectDoesNotExist
from django.conf import settings
from django.core import serializers as django_serializers
from django.core.validators import validate_comma_separated_integer_list

from argo import QueueMessage,ArgoJobStatus
from common import log_uncaught_exceptions,MessageInterface
from common import Serializer,transfer,Mail,db_tools
from balsam.models import BalsamJob
from balsam.models import STATES_BY_NAME as BALSAM_STATES_BY_NAME

# assign this function to the system exception hook
sys.excepthook = log_uncaught_exceptions.log_uncaught_exceptions

    
def submit_subjob(job):
   logger.debug('in submit_subjob pk=' + str(job.pk) + ' argo_job_id='+str(job.argo_job_id))
   message = 'Subjob submitted'
   try:
      # get the current subjob
      subjob = job.get_current_subjob()

      # use subjob to fill BalsamJobMessage that will be sent to balsam
      #balsamJobMsg = subjob.get_balsam_job_message()
     
      # determine site name
      logger.info('Submitting Subjob ' + str(subjob.balsam_job_id) + ' from ArgoJob ' 
            + str(job.argo_job_id) + ' (pk=' + str(job.pk) + ') to ' + subjob.site )

      # create and configure message interface
      msgInt = MessageInterface.MessageInterface(
                host          = settings.RABBITMQ_SERVER_NAME,
                port          = settings.RABBITMQ_SERVER_PORT,
                exchange_name = settings.RABBITMQ_BALSAM_EXCHANGE_NAME,
                ssl_cert      = settings.RABBITMQ_SSL_CERT,
                ssl_key       = settings.RABBITMQ_SSL_KEY,
                ssl_ca_certs  = settings.RABBITMQ_SSL_CA_CERTS,
               )
      # opening blocking connection which will close at the end of this function
      msgInt.open_blocking_connection()

      # create message queue for site in case not already done
      msgInt.create_queue(subjob.site,subjob.site)

      # serialize subjob for message
      body = django_serializers.serialize('json',[subjob])

      # submit job
      msgInt.send_msg(body,subjob.site)
      # close connection
      msgInt.close()
      
      job.state = SUBJOB_SUBMITTED.name
   except SubJobIndexOutOfRange:
      message = 'All Subjobs Completed'
      job.state = SUBJOBS_COMPLETED.name
   except Exception,e:
      message = ('Exception received while submitting subjob to ' 
         + subjob.site + ' for job pk=' + str(job.pk) + ' argo_id=' 
         + str(job.argo_job_id) + ': ' + str(e))
      logger.exception(message)
      job.state = SUBJOB_SUBMIT_FAILED.name

   job.save(update_fields=['state'],using=db_tools.get_db_connection_id(job.pk))
   send_status_message(job,message)

def increment_subjob(job):
   ''' increments subjob index '''
   logger.debug('in increment subjob pk='+str(job.pk))
   message = 'subjob incremented'
   job.current_subjob_pk_index += 1
   logger.debug(' setting current_subjob_pk_index = ' + str(job.current_subjob_pk_index))
   job.state = SUBJOB_INCREMENTED.name
   job.save(update_fields=['state','current_subjob_pk_index'],
            using=db_tools.get_db_connection_id(job.pk))


def stage_in(job):
   ''' stages data in from the user if an input_url is specified '''
   logger.debug('in stage_in pk=' + str(job.pk))
   message = 'Job staged in'
   if job.input_url != '':
      try:
         transfer.stage_in(job.input_url + '/',job.working_directory + '/')
         job.state = STAGED_IN.name
      except Exception,e:
         message = 'Exception received during stage_in: ' + str(e)
         logger.exception(message)
         job.state = STAGE_IN_FAILED.name
   else:
      # no input url specified so stage in is complete
      job.state = STAGED_IN.name
   
   job.save(update_fields=['state'],using=db_tools.get_db_connection_id(job.pk))


def stage_out(job):
   ''' stages data out to the user if an output_url is specified '''
   logger.debug('in stage_out pk=' + str(job.pk))
   message = 'Job staged out'
   if job.output_url != '':
      try:
         transfer.stage_out(str(job.working_directory) + '/', str(job.output_url) + '/')
         job.state = STAGED_OUT.name
      except Exception,e:
         message = 'Exception received during stage_out: ' + str(e)
         logger.exception(message)
         job.state = STAGE_OUT_FAILED.name
   else:
      # no input url specified so stage in is complete
      job.state = STAGED_OUT.name

   job.save(update_fields=['state'],using=db_tools.get_db_connection_id(job.pk))


def make_history(job):
   logger.debug('job ' + str(job.pk) + ' in make_history ')
   job.state = HISTORY.name
   job.save(update_fields=['state'],using=db_tools.get_db_connection_id(job.pk))
      
def send_status_message(job,message=None):
   ''' this function sends status messages back to the users via email and message queue '''
   logger.debug('in send_status_message pk=' + str(job.pk) + ' argo_job_id='+str(job.argo_job_id))
   try:
      receiver = ''
      if len(job.email) > 0 and '@' in job.email:
         receiver = job.email
      else:
         logger.warning(' no email address specified, not sending mail, email=' + str(job.email))
         return
      
      # construct body of email
      body = ' Your job has reached state ' + job.state + '\n'
      if message is not None:
        body += '    with the message: ' + str(message)
      body += '------------------------------------------------------------------- \n'
      body += 'Job Data: \n'
      body += django_serializers.serialize('json',[job])
      body += '------------------------------------------------------------------- \n'
      body += 'Subjob Data: \n'
      body += django_serializers.serialize('json',ArgoSubJob.objects.filter(pk__in=Serializer.deserialize(job.subjob_pk_list)))
      
      # send notification email to user
      Mail.send_mail(
                sender    = settings.ARGO_JOB_STATUS_EMAIL_SENDER,
                receiver  = receiver,
                subject   = 'ARGO Job Status Report',
                body      = body,
               )
   except Exception,e:
      logger.exception('exception received while trying to send status email. Exception: ' + str(e))

   # if job has an argo job status routing key, send a message there
   if job.job_status_routing_key != '' and send_status_message:
      logger.info('sending job status message with routing key: ' + job.job_status_routing_key)
      try:
         msg = ArgoJobStatus.ArgoJobStatus()
         msg.state = job.state
         msg.message = message
         msg.job_id = job.argo_job_id
         mi                = MessageInterface.MessageInterface()
         mi.host           = settings.RABBITMQ_SERVER_NAME
         mi.port           = settings.RABBITMQ_SERVER_PORT
         mi.exchange_name  = settings.RABBITMQ_USER_EXCHANGE_NAME

         mi.ssl_cert       = settings.RABBITMQ_SSL_CERT
         mi.ssl_key        = settings.RABBITMQ_SSL_KEY
         mi.ssl_ca_certs   = settings.RABBITMQ_SSL_CA_CERTS
         logger.debug( ' open blocking connection to send status message ' )
         mi.open_blocking_connection()
         mi.send_msg(msg.get_serialized_message(),job.job_status_routing_key)
         mi.close()
      except:
         logger.exception('Exception while sending status message to user job queue')



# ------------  Job States ----------------------------

from common.JobState import JobState

# Job States
CREATE_FAILED              = JobState('CREATE_FAILED')
CREATED                    = JobState('CREATED',CREATE_FAILED,stage_in)
STAGE_IN_FAILED            = JobState('STAGE_IN_FAILED')
STAGED_IN                  = JobState('STAGED_IN',STAGE_IN_FAILED,submit_subjob)

SUBJOB_SUBMITTED           = JobState('SUBJOB_SUBMITTED')
SUBJOB_SUBMIT_FAILED       = JobState('SUBJOB_SUBMIT_FAILED')
SUBJOB_IN_PREPROCESS       = JobState('SUBJOB_IN_PREPROCESS')
SUBJOB_PREPROCESS_FAILED   = JobState('SUBJOB_PREPROCESS_FAILED')
SUBJOB_QUEUED              = JobState('SUBJOB_QUEUED')
SUBJOB_RUNNING             = JobState('SUBJOB_RUNNING')
SUBJOB_RUN_FINISHED        = JobState('SUBJOB_RUN_FINISHED')
SUBJOB_RUN_FAILED          = JobState('SUBJOB_RUN_FAILED')
SUBJOB_IN_POSTPROCESS      = JobState('SUBJOB_IN_POSTPROCESS')
SUBJOB_POSTPROCESS_FAILED  = JobState('SUBJOB_POSTPROCESS_FAILED')

SUBJOB_COMPLETE_FAILED     = JobState('SUBJOB_COMPLETE_FAILED')
SUBJOB_COMPLETED           = JobState('SUBJOB_COMPLETED',SUBJOB_COMPLETE_FAILED,increment_subjob)
SUBJOB_REJECTED            = JobState('SUBJOB_REJECTED')

SUBJOB_INCREMENT_FAILED    = JobState('SUBJOB_INCREMENT_FAILED')
SUBJOB_INCREMENTED         = JobState('SUBJOB_INCREMENTED',SUBJOB_INCREMENT_FAILED,submit_subjob)

SUBJOBS_COMPLETED          = JobState('SUBJOBS_COMPLETED',stage_out)

STAGE_OUT_FAILED           = JobState('STAGE_OUT_FAILED')
STAGED_OUT                 = JobState('STAGED_OUT',STAGE_OUT_FAILED,make_history)
HISTORY                    = JobState('HISTORY')
FAILED                     = JobState('FAILED')
REJECTED                   = JobState('REJECTED')


STATES  = [
   CREATED,
   CREATE_FAILED,
   STAGED_IN,
   STAGE_IN_FAILED,

   SUBJOB_SUBMITTED,
   SUBJOB_SUBMIT_FAILED,
   SUBJOB_IN_PREPROCESS,
   SUBJOB_PREPROCESS_FAILED,
   SUBJOB_QUEUED,
   SUBJOB_RUNNING,
   SUBJOB_RUN_FINISHED,
   SUBJOB_RUN_FAILED,
   SUBJOB_IN_POSTPROCESS,
   SUBJOB_POSTPROCESS_FAILED,
   SUBJOB_COMPLETED,
   SUBJOB_COMPLETE_FAILED,
   SUBJOB_REJECTED,
   SUBJOB_INCREMENTED,
   SUBJOB_INCREMENT_FAILED,
   SUBJOBS_COMPLETED,

   STAGED_OUT,
   STAGE_OUT_FAILED,
   HISTORY,
   FAILED,
   REJECTED,
]

TRANSITIONABLE_STATES = []
for state in STATES:
   if state.transition_function is not None:
      TRANSITIONABLE_STATES.append(state.name)

STATES_BY_NAME = { x.name:x for x in STATES }

BALSAM_JOB_TO_SUBJOB_STATE_MAP = {
   BALSAM_STATES_BY_NAME['CREATED'].name:SUBJOB_IN_PREPROCESS,
   BALSAM_STATES_BY_NAME['CREATE_FAILED'].name:SUBJOB_PREPROCESS_FAILED,
   BALSAM_STATES_BY_NAME['STAGED_IN'].name:SUBJOB_IN_PREPROCESS,
   BALSAM_STATES_BY_NAME['STAGE_IN_FAILED'].name:SUBJOB_PREPROCESS_FAILED,
   BALSAM_STATES_BY_NAME['PREPROCESSED'].name:SUBJOB_IN_PREPROCESS,
   BALSAM_STATES_BY_NAME['PREPROCESS_FAILED'].name:SUBJOB_PREPROCESS_FAILED,
   BALSAM_STATES_BY_NAME['SUBMITTED'].name:SUBJOB_IN_PREPROCESS,
   BALSAM_STATES_BY_NAME['SUBMIT_FAILED'].name:SUBJOB_PREPROCESS_FAILED,
   BALSAM_STATES_BY_NAME['SUBMIT_DISABLED'].name:SUBJOB_COMPLETED,

   BALSAM_STATES_BY_NAME['QUEUED'].name:SUBJOB_QUEUED,
   BALSAM_STATES_BY_NAME['RUNNING'].name:SUBJOB_RUNNING,
   BALSAM_STATES_BY_NAME['EXECUTION_FINISHED'].name:SUBJOB_RUN_FINISHED,
   BALSAM_STATES_BY_NAME['EXECUTION_FAILED'].name:SUBJOB_RUN_FAILED,

   BALSAM_STATES_BY_NAME['POSTPROCESSED'].name:SUBJOB_IN_POSTPROCESS,
   BALSAM_STATES_BY_NAME['POSTPROCESS_FAILED'].name:SUBJOB_POSTPROCESS_FAILED,
   BALSAM_STATES_BY_NAME['STAGED_OUT'].name:SUBJOB_IN_POSTPROCESS,
   BALSAM_STATES_BY_NAME['STAGE_OUT_FAILED'].name:SUBJOB_POSTPROCESS_FAILED,

   BALSAM_STATES_BY_NAME['JOB_FINISHED'].name:SUBJOB_COMPLETED,
   BALSAM_STATES_BY_NAME['JOB_FAILED'].name:SUBJOB_COMPLETE_FAILED,
   BALSAM_STATES_BY_NAME['JOB_REJECTED'].name:SUBJOB_REJECTED,
}


# -------------   ArgoJob DB Object ----------------------

import time,os,shutil
#from django.db import models

class SubJobIndexOutOfRange(Exception): pass
class ArgoJob(models.Model):
   
   # ARGO DB table columns
   argo_job_id             = models.BigIntegerField(default=0)
   user_id                 = models.BigIntegerField(default=0)
   name                    = models.TextField(default='')
   description             = models.TextField(default='')
   group_identifier        = models.TextField(default='')

   working_directory       = models.TextField(default='')
   time_created            = models.DateTimeField(auto_now_add=True)
   time_modified           = models.DateTimeField(auto_now=True)
   time_finished           = models.DateTimeField(null=True)
   state                   = models.TextField(default=CREATED.name)
   username                = models.TextField(default='')
   email                   = models.TextField(default='')

   input_url               = models.TextField(default='')
   output_url              = models.TextField(default='')

   subjob_pk_list          = models.TextField(default='',validators=[validate_comma_separated_integer_list])
   current_subjob_pk_index = models.IntegerField(default=0)

   job_status_routing_key  = models.TextField(default='')

   def get_current_subjob(self):
      subjob_list = self.get_subjob_pk_list()
      if self.current_subjob_pk_index < len(subjob_list):
         logger.debug('getting subjob index ' + str(self.current_subjob_pk_index) + ' of ' + str(len(subjob_list)))
         return ArgoSubJob.objects.get(pk=subjob_list[self.current_subjob_pk_index])
      else:
         logger.debug('current_subjob_pk_index=' + str(self.current_subjob_pk_index) + ' number of subjobs = ' + str(len(subjob_list)) + ' subjobs = ' + str(subjob_list))
         raise SubJobIndexOutOfRange

   def add_subjob(self,subjob):
      subjob_list = self.get_subjob_pk_list()
      subjob_list.append(subjob.pk)
      self.subjob_pk_list = Serializer.serialize(subjob_list)

   def get_subjob_pk_list(self):
      return Serializer.deserialize(self.subjob_pk_list)

   def get_line_string(self):
      format = " %10i | %20i | %20s | %35s | %15s | %20s "
      output = format % (self.pk,self.argo_job_id,self.state,str(self.time_modified),self.username,self.subjob_pk_list)
      return output

   @staticmethod
   def get_header():
      format = " %10s | %20s | %20s | %35s | %15s | %20s "
      output = format % ('pk','argo_job_id','state','time_modified','username','subjob_pk_list')
      return output

   @staticmethod
   def generate_job_id():
      # time.time() is a double with units seconds
      # so grabing the number of microseconds
      job_id = int(time.time()*1e6)
      # make sure no jobs with the same job_id
      same_jobs = ArgoJob.objects.filter(argo_job_id=job_id)
      while len(same_jobs) > 0:
         job_id = int(time.time()*1e6)
         same_jobs = ArgoJob.objects.filter(argo_job_id=job_id)
      return job_id

   def delete(self):
      # delete local argo job path
      if os.path.exists(self.working_directory):
         try:
            shutil.rmtree(self.working_directory)
            logger.info('removed job path: ' + str(self.working_directory))
         except Exception,e:
            logger.error('Error trying to remove argo job path: ' + str(self.working_directory) + ' Exception: ' + str(e))

      # call base class delete function
      try:
         super(ArgoJob,self).delete()
      except Exception,e:
         logger.error('pk='+str(self.pk) + ' Received exception during "delete": ' + str(e))

# must do this to force django to create a DB table for ARGO independent of the one created for Balsam
class ArgoSubJob(BalsamJob): pass

'''
class ArgoSubJob(models.Model):

   # ArgoSubJob DB table columns
   site                    = models.TextField(default='')
   state                   = models.TextField(default='PRESUBMIT')

   name                    = models.TextField(default='')
   description             = models.TextField(default='')
   subjob_id               = models.BigIntegerField(default=0)
   argo_job_id             = models.BigIntegerField(default=0)

   queue                   = models.TextField(default=settings.BALSAM_DEFAULT_QUEUE)
   project                 = models.TextField(default=settings.BALSAM_DEFAULT_PROJECT)
   wall_time_minutes       = models.IntegerField(default=0)
   num_nodes               = models.IntegerField(default=0)
   processes_per_node      = models.IntegerField(default=0)
   scheduler_config        = models.TextField(default='None')
   scheduler_id            = models.IntegerField(default=0)
   
   application             = models.TextField(default='')
   config_file             = models.TextField(default='')

   input_url               = models.TextField(default='')
   output_url              = models.TextField(default='')

   def get_balsam_job_message(self):
      msg = BalsamJobMessage.BalsamJobMessage()
      msg.origin_id           = self.subjob_id
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

   def get_line_string(self):
      format = ' %10i | %20i | %20i | %10s | %20s | %10i | %10i | %10s | %10s | %10s | %15s '
      output = format % (self.pk,self.subjob_id,self.argo_job_id,self.state,self.site,
            self.num_nodes,self.processes_per_node,self.scheduler_id,self.queue,
            self.project,self.application)
      return output

   @staticmethod
   def get_header():
      format = ' %10s | %20s | %20s | %10s | %20s | %10s | %10s | %10s | %10s | %10s | %15s '
      output = format % ('pk','subjob_id','argo_job_id','state','site',
            'num_nodes','procs','sched_id','queue','project','application')
      return output
'''
