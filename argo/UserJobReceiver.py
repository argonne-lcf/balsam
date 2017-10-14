import logging,sys,multiprocessing,time,os,pwd,grp
logger = logging.getLogger(__name__)

from django.db import connections,DEFAULT_DB_ALIAS
from django.db.utils import load_backend
from django.conf import settings

from argo import models,QueueMessage
from common import db_tools
from common import MessageReceiver,Serializer

def CreateWorkingPath(job_id):
   path = os.path.join(settings.ARGO_WORK_DIRECTORY,str(job_id))
   os.makedirs(path)
   return path

class UserJobReceiver(MessageReceiver.MessageReceiver):
   ''' subscribes to the input user job queue and adds jobs to the database '''

   def __init__(self,process_queue = None):
      super(UserJobReceiver,self).__init__(
            settings.RABBITMQ_USER_JOB_QUEUE_NAME,
            settings.RABBITMQ_USER_JOB_ROUTING_KEY,
            settings.RABBITMQ_SERVER_NAME,
            settings.RABBITMQ_SERVER_PORT,
            settings.RABBITMQ_USER_EXCHANGE_NAME,
            settings.RABBITMQ_SSL_CERT,
            settings.RABBITMQ_SSL_KEY,
            settings.RABBITMQ_SSL_CA_CERTS,
           )
      self.process_queue = process_queue
            

   
   # This is where the real processing of incoming messages happens
   def consume_msg(self,channel,method_frame,header_frame,body):
      logger.debug('in consume_msg')
      if body is not None:
         logger.debug(' received message: ' + body )

         # convert body text to ArgoUserJob
         try:
            userjob = Serializer.deserialize(body)
         except Exception as e:
            logger.error(' received exception while deserializing message to create ArgoUserJob, \nexception message: ' + str(e) + '\n message body: \n' + body + ' \n cannot continue with this job, ignoring it and moving on.')
            # acknoledge message
            channel.basic_ack(method_frame.delivery_tag)
            return
         
         # create unique DB connection string
         try:
            db_connection_id = db_tools.get_db_connection_id(os.getpid())
            db_backend = load_backend(connections.databases[DEFAULT_DB_ALIAS]['ENGINE'])
            db_conn = db_backend.DatabaseWrapper(connections.databases[DEFAULT_DB_ALIAS], db_connection_id)
            connections[db_connection_id] = db_conn
         except Exception as e:
            logger.error(' received exception while creating DB connection, exception message: ' + str(e) + ' \n job id: ' + str(userjob['user_id']) + ' job user: ' + userjob['username'] + ' job description: ' + userjob['description'] + '\n cannot continue with this job, moving on.')
            # acknoledge message
            channel.basic_ack(method_frame.delivery_tag)
            return

         # create ArgoJob and initialize it
         try:
            argojob = models.ArgoJob()
            argojob.job_id              = models.ArgoJob.generate_job_id()
            logger.debug(' created ArgoJob with id: ' + str(argojob.job_id) )
            argojob.working_directory        = CreateWorkingPath(argojob.job_id)
            argojob.user_id                  = userjob['user_id']
            argojob.job_name                 = userjob['name']
            argojob.job_description          = userjob['description']
            argojob.group_identifier         = userjob['group_identifier']
            argojob.username                 = userjob['username']
            argojob.email                    = userjob['email']
            argojob.input_url                = userjob['input_url']
            argojob.output_url               = userjob['output_url']
            argojob.job_status_routing_key   = userjob['job_status_routing_key']

            # if there are no subjobs, there isn't anything to do
            if len(userjob['subjobs']) == 0:
               logger.error(' Job received with no subjobs, failing job and moving on.')
               argojob.state_id = models.REJECTED.id
               argojob.save()
               message = 'Job rejected because there are no subjobs.'
               models.send_status_message(job,message)
               # acknoledge message
               channel.basic_ack(method_frame.delivery_tag)
               del connections[db_connection_id]
               return

            # add subjobs
            subjob_pks = []
            for usersubjob in userjob['subjobs']:
               argosubjob                       = models.ArgoSubJob()
               argosubjob.site                  = usersubjob['site']
               argosubjob.job_id                = models.ArgoJob.generate_job_id()
               argosubjob.name                  = usersubjob['name']
               argosubjob.description           = usersubjob['description']
               argosubjob.argo_job_id           = argojob.job_id
               argosubjob.queue                 = usersubjob['queue']
               argosubjob.project               = usersubjob['project']
               argosubjob.wall_time_minutes     = usersubjob['wall_time_minutes']
               argosubjob.num_nodes             = usersubjob['num_nodes']
               argosubjob.processes_per_node    = usersubjob['processes_per_node']
               argosubjob.scheduler_config      = usersubjob['scheduler_config']
               argosubjob.application           = usersubjob['application']
               argosubjob.config_file           = usersubjob['config_file']
               argosubjob.input_url =  (
                     settings.GRIDFTP_PROTOCOL + 
                     settings.GRIDFTP_SERVER + 
                     argojob.working_directory
                    )
               argosubjob.output_url            = argosubjob.input_url
               argosubjob.save()
               subjob_pks.append(argosubjob.pk)
            argojob.subjob_pk_list = Serializer.serialize(subjob_pks)
            argojob.save()
            self.process_queue.put(QueueMessage.QueueMessage(argojob.pk,0,'new job received'))
         except Exception as e:
            message = 'received an exception while parsing the incomping user job. Exception: ' + str(e) + '; userjob id = ' + str(userjob['user_id']) + '; job_id = ' + str(argojob.job_id) + '; job_name = ' + userjob['name']
            logger.error(message)

         # delete DB connection
         del connections[db_connection_id]
         logger.debug('added user job')
      else:
         logger.error('received user job message with no body')
      # acknoledge message
      channel.basic_ack(method_frame.delivery_tag)
