from common.MessageInterface import MessageInterface
from django.conf import settings
import logging,sys,multiprocessing,time,os
logger = logging.getLogger(__name__)

QUEUE_NAME  = None
ROUTING_KEY = None

class MessageReceiver(multiprocessing.Process):
   ''' subscribes to a queue and executes the given callback'''

   # this method should be re-defined by the user via inheritance
   def consume_msg(self,channel,method_frame,header_frame,body):
      pass
  
   def __init__(self,
                msg_queue,
                msg_routing_key,
                msg_host,
                msg_port,
                msg_exchange_name,
                msg_ssl_cert,
                msg_ssl_key,
                msg_ssl_ca_certs,
               ):
      # execute super constructor
      super(MessageReceiver,self).__init__()
      
      #self.exit = multiprocessing.Event()
      self.messageInterface = MessageInterface()
      self.messageInterface.host          = msg_host
      self.messageInterface.port          = msg_port
      self.messageInterface.exchange_name = msg_exchange_name

      self.messageInterface.ssl_cert      = msg_ssl_cert
      self.messageInterface.ssl_key       = msg_ssl_key
      self.messageInterface.ssl_ca_certs  = msg_ssl_ca_certs

      self.message_queue                  = msg_queue
      self.message_routing_key            = msg_routing_key
   
   def run(self):
      logger.debug(' in run ')
      
      # setup receiving queue and exchange
      logger.debug( ' open blocking connection to setup queue ' )
      self.messageInterface.open_blocking_connection()
      self.messageInterface.create_queue(self.message_queue,self.message_routing_key)
      self.messageInterface.close()
      
      logger.debug( ' open select connection ' )
      # start consuming incoming messages
      try:
         self.messageInterface.open_select_connection(self.on_connection_open)
      except:
         logger.exception(' Received exception while opening select connection: ' + str(sys.exc_info()[1]))
         raise
      
      logger.debug( ' start message consumer ' )
      try:
         self.messageInterface.connection.ioloop.start()
      except:
         logger.exception(' Received exception while starting ioloop for message consumer: ' + str(sys.exc_info()[1]))
         raise
      
   
   # not working... connection is None for some reason
   def shutdown(self):
      logger.debug(' stopping message consumer ')
      try:
         logger.debug(' message connection: ' + str(self.messageInterface.connection) )
         logger.debug(' message ioloop: ' + str(self.messageInterface.connection.ioloop) )
         self.messageInterface.connection.ioloop.stop()
         logger.debug( ' after stopping message consumer ')
      except:
         logger.exception(' Received exception while stopping ioloop for the message consumer: ' + str(sys.exc_info()[1]))
         raise
      #self.exit.set()
   
   def on_connection_open(self,connection):
      logger.debug(' in on_connection_open')
      try:
         connection.channel(self.on_channel_open)
      except:
         logger.exception(' Received exception while opening connection to message server: ' + str(sys.exc_info()[1]))
         raise
   
   def on_channel_open(self,channel):
      logger.debug(' in on_channel_open')
      try:
         channel.basic_consume(self.consume_msg,self.message_queue)
      except:
         logger.exception(' Received exception while creating message consumer: ' + str(sys.exc_info()[1]))
         raise
   



