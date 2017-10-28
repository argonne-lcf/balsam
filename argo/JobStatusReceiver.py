import logging,sys,multiprocessing,time,os
logger = logging.getLogger(__name__)

from django.db import connections,DEFAULT_DB_ALIAS
from django.db.utils import load_backend
from django.conf import settings

from common import MessageReceiver
from argo import QueueMessage
from argo.models import ArgoJob,ArgoSubJob,BALSAM_JOB_TO_SUBJOB_STATE_MAP
from balsam import BalsamJobStatus,models

class JobStatusReceiver(MessageReceiver.MessageReceiver):
   ''' subscribes to the balsam job status queue and updates a job state '''

   def __init__(self,process_queue):
      super(JobStatusReceiver,self).__init__(
            settings.RABBITMQ_BALSAM_JOB_STATUS_QUEUE,
            settings.RABBITMQ_BALSAM_JOB_STATUS_ROUTING_KEY,
            settings.RABBITMQ_SERVER_NAME,
            settings.RABBITMQ_SERVER_PORT,
            settings.RABBITMQ_BALSAM_EXCHANGE_NAME,
            settings.RABBITMQ_SSL_CERT,
            settings.RABBITMQ_SSL_KEY,
            settings.RABBITMQ_SSL_CA_CERTS
           )
      self.process_queue = process_queue

   # This is where the real processing of incoming messages happens
   def consume_msg(self,channel,method_frame,header_frame,body):
      logger.debug(' in consume_msg ')
      try:
         if body is not None:

            # convert body text to BalsamJobStatusMessage
            statusMsg = BalsamJobStatus.BalsamJobStatus()
            statusMsg.deserialize(body)
            logger.info(' received status message for job ' + str(statusMsg.job_id) + ', message: ' + str(statusMsg.message))
            
            # create unique DB connection string
            db_connection_id = 'db_con_%08i' % statusMsg.job_id
            db_backend = load_backend(connections.databases[DEFAULT_DB_ALIAS]['ENGINE'])
            db_conn = db_backend.DatabaseWrapper(connections.databases[DEFAULT_DB_ALIAS], db_connection_id)
            connections[db_connection_id] = db_conn
            
            # get the subjob for this message
            try:
               subjob = ArgoSubJob.objects.get(job_id=statusMsg.job_id)
            except Exception as e:
               logger.error(' exception received while retreiving ArgoSubJob with id = ' + str(statusMsg.job_id) + ': ' + str(e))
               # acknoledge message
               channel.basic_ack(method_frame.delivery_tag)
               del connections[db_connection_id]
               # send message to balsam_service about completion
               self.process_queue.put(QueueMessage.QueueMessage(statusMsg.job_id,
                              QueueMessage.JobStatusReceiverRetrieveArgoSubJobFailed))
               return

            # get the argo job for this subjob
            try:
               argojob = ArgoJob.objects.get(job_id=subjob.job_id) # BUG !
            except Exception as e:
               logger.error(' exception received while retrieving ArgoJob with id = ' + str(subjob.job_id + ': ' + str(e)))
               # acknoledge message
               channel.basic_ack(method_frame.delivery_tag)
               del connections[db_connection_id]
               # send message to balsam_service about completion
               self.process_queue.put(QueueMessage.QueueMessage(subjob.job_id,
                              QueueMessage.JobStatusReceiverRetrieveArgoJobFailed))
               return

            # get the deserialized balsam job
            try:
               balsam_job = models.BalsamJob()
               statusMsg.get_job(balsam_job) # statusMsg.serialzed_job gets loaded into balsam_job
               logger.debug('balsam_job = ' + str(balsam_job))
            except BalsamJobStatus.DeserializeFailed as e:
               logger.error('Failed to deserialize BalsamJob for BalsamJobStatus message for argojob: ' + str(argojob.job_id) + ' subjob_id: ' + str(subjob.job_id))
               # acknoledge message
               channel.basic_ack(method_frame.delivery_tag)
               del connections[db_connection_id]
               # send message to balsam_service about completion
               self.process_queue.put(QueueMessage.QueueMessage(subjob.job_id,
                              QueueMessage.JobStatusReceiverRetrieveArgoJobFailed))
               return

            # parse balsam_job (just received from balsam, new status) into
            # subjob and argojob (need to be synced)
            if balsam_job is not None:

               # copy scheduler id to subjob
               subjob.scheduler_id  = balsam_job.scheduler_id
               # copy current job state to subjob
               subjob.state         = balsam_job.state
               # save subjob
               subjob.save(update_fields=['state','scheduler_id'],using=db_connection_id)
               
               # map subjob state to argo job state
               try:
                  argojob.state = BALSAM_JOB_TO_SUBJOB_STATE_MAP[balsam_job.state].name
                  logger.debug(' receieved subjob state = ' + subjob.state + ' setting argo job state to ' + argojob.state)
               except KeyError as e:
                  logger.error(' could not map balsam_job state: ' + str(balsam_job.state) + ' to an ArgoJob state for job id: ' + str(argojob.job_id))
                  # acknoledge message
                  channel.basic_ack(method_frame.delivery_tag)
                  del connections[db_connection_id]
                  # send message to balsam_service about completion
                  self.process_queue.put(QueueMessage.QueueMessage(argojob.job_id,
                                 QueueMessage.JobStatusReceiverBalsamStateMapFailure))
                  return
            
               # save argojob
               argojob.save(update_fields=['state'],using=db_connection_id)

            else:
               logger.error('received no balsam_job from BalsamJobStatus')
            self.process_queue.put(QueueMessage.QueueMessage(argojob.job_id,
                           QueueMessage.JobStatusReceiverCompleted))
            # acknoledge message
            channel.basic_ack(method_frame.delivery_tag)

            del connections[db_connection_id]



         else:
            logger.debug(' consume_msg called, but body is None ')
            self.process_queue.put(QueueMessage.QueueMessage(argojob.job_id,
                           QueueMessage.JobStatusReceiverMessageNoBody))
      
      except Exception as e:
         logger.exception("Error consuming status message: " + str(e))
         self.process_queue.put(QueueMessage.QueueMessage(0,
                        QueueMessage.JobStatusReceiverFailed))
      
      logger.debug(' leaving consume_msg ' )

