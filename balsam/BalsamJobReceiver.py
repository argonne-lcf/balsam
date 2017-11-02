import logging,sys,os
logger = logging.getLogger(__name__)

from common import MessageReceiver,db_tools
from balsam import models, BalsamStatusSender

from django.conf import settings
from django.db import utils,connections,DEFAULT_DB_ALIAS

class BalsamJobReceiver(MessageReceiver.MessageReceiver):
   ''' subscribes to the input user job queue and adds jobs to the database '''

   def __init__(self, receiver_settings):
      MessageReceiver.MessageReceiver.__init__(self, receiver_settings)
   
   # This is where the real processing of incoming messages happens
   # It is invoked by the parent MessageReceiver's protocol-specific
   # message consumer
   def handle_msg(self, msg_body):
       logger.debug('in handle_msg' )
       try:
            job = models.BalsamJob()
            job.deserialize(body)
       except Exception as e:
            logger.exception('error deserializing incoming job. body = ' + body + ' not conitnuing with this job.')
            raise Exception("Deserialize failed")
       try:
            db_connection_id = db_tools.get_db_connection_id(os.getpid())
            db_backend = utils.load_backend(connections.databases[DEFAULT_DB_ALIAS]['ENGINE'])
            db_conn = db_backend.DatabaseWrapper(connections.databases[DEFAULT_DB_ALIAS], db_connection_id)
            connections[db_connection_id] = db_conn
       except Exception as e:
           raise Exception("received exception while creating DB connection")

       job.save()
       del connections[db_connection_id]
       status_sender = BalsamStatusSender.BalsamStatusSender(settings.SENDER_CONFIG)
       status_sender.send_status(job)
