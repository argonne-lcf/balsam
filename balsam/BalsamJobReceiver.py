import logging,sys,os
logger = logging.getLogger(__name__)

from common import MessageReceiver,db_tools
from balsam import models

from django.core import serializers
from django.conf import settings
from django.db import utils,connections,DEFAULT_DB_ALIAS

class BalsamJobReceiver(MessageReceiver.MessageReceiver):
   ''' subscribes to the input user job queue and adds jobs to the database '''

   def __init__(self):
      MessageReceiver.MessageReceiver.__init__(self,
            settings.BALSAM_SITE,
            settings.BALSAM_SITE,
            settings.RABBITMQ_SERVER_NAME,
            settings.RABBITMQ_SERVER_PORT,
            settings.RABBITMQ_BALSAM_EXCHANGE_NAME,
            settings.RABBITMQ_SSL_CERT,
            settings.RABBITMQ_SSL_KEY,
            settings.RABBITMQ_SSL_CA_CERTS
           )
   
   # This is where the real processing of incoming messages happens
   def consume_msg(self,channel,method_frame,header_frame,body):
      logger.debug('in consume_msg' )
      if body is not None:

         logger.debug(' received message: ' + body )
         try:
            jobs = serializers.deserialize('json',body)
         except Exception,e:
            logger.error('error deserializing incoming job. body = ' + body + ' not conitnuing with this job.')
            channel.basic_ack(method_frame.delivery_tag)
            return
         # should be some failure notice to argo

         # create unique DB connection string
         try:
            db_connection_id = db_tools.get_db_connection_id(os.getpid())
            db_backend = utils.load_backend(connections.databases[DEFAULT_DB_ALIAS]['ENGINE'])
            db_conn = db_backend.DatabaseWrapper(connections.databases[DEFAULT_DB_ALIAS], db_connection_id)
            connections[db_connection_id] = db_conn
         except Exception,e:
            logger.error(' received exception while creating DB connection, exception message: ' + str(e))
            # acknoledge message
            channel.basic_ack(method_frame.delivery_tag)
            return

         for job in jobs:
            job.save()
            models.send_status_message(job)

      else:
         logger.error(' consume_msg called, but body is None ')
         # should be some failure notice to argo

      # acknowledge receipt of message
      channel.basic_ack(method_frame.delivery_tag)
      # delete DB connection
      del connections[db_connection_id]

