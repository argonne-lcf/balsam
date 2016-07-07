import sys,os,ssl
import pika,time

import logging
logger = logging.getLogger(__name__)
logging.getLogger('pika').setLevel(logging.WARNING)
#logging.getLogger('select_connection').setLevel(logging.DEBUG)

class MessageInterface:
   
   def __init__(self,
                username                  = '',
                password                  = '',
                host                      = '',
                port                      = -1,
                virtual_host              = '/',
                socket_timeout            = 120,
                exchange_name             = '',
                exchange_type             = 'topic',
                exchange_durable          = True,
                exchange_auto_delete      = False,
                ssl_cert                  = '',
                ssl_key                   = '',
                ssl_ca_certs              = '',
                queue_is_durable          = True,
                queue_is_exclusive        = False,
                queue_is_auto_delete      = False,
               ):
      self.username                 = username
      self.password                 = password
      self.host                     = host
      self.port                     = port
      self.virtual_host             = virtual_host
      self.socket_timeout           = socket_timeout
      self.exchange_name            = exchange_name
      self.exchange_type            = exchange_type
      self.exchange_durable         = exchange_durable
      self.exchange_auto_delete     = exchange_auto_delete
      self.queue_is_durable         = queue_is_durable
      self.queue_is_exclusive       = queue_is_exclusive
      self.queue_is_auto_delete     = queue_is_auto_delete

      self.ssl_cert                 = ssl_cert
      self.ssl_key                  = ssl_key
      self.ssl_ca_certs             = ssl_ca_certs

      self.credentials = None
      self.parameters = None
      self.connection = None
      self.channel = None


   def open_blocking_connection(self):
      
      logger.debug("open blocking connection")
      self.create_connection_parameters()

      # open the connection and grab the channel
      try:
         self.connection = pika.BlockingConnection(self.parameters)
      except:
         logger.exception(' Exception received while trying to open blocking connection to message server')
         raise

      try:
         self.channel   = self.connection.channel()
      except:
         logger.exception(' Exception received while trying to open a channel to the message server')
         raise
      
      logger.debug("create exchange, name = " + self.exchange_name) 
      # make sure exchange exists (doesn't do anything if already created)
      self.channel.exchange_declare(
                                    exchange       = self.exchange_name,
                                    exchange_type  = self.exchange_type,
                                    durable        = self.exchange_durable,
                                    auto_delete    = self.exchange_auto_delete,
                                   )

   def open_select_connection(self,
                              on_open_callback              = None,
                              on_open_error_callback        = None,
                              on_close_callback             = None,
                              stop_ioloop_on_close          = True,
                             ):
      logger.debug("create select connection")
      self.create_connection_parameters()
      # open the connection
      if on_open_callback is not None:
         try:
            self.connection = pika.SelectConnection(self.parameters,
                                                    on_open_callback,
                                                    on_open_error_callback,
                                                    on_close_callback,
                                                    stop_ioloop_on_close,
                                                   )
         except:
            logger.error(' Exception received while trying to open select connection to message server: ' + str(sys.exc_info()))
            raise

   def create_connection_parameters(self):
      logger.debug("create connection parameters, server = " + self.host + " port = " + str(self.port))
      # need to set credentials to login to the message server
      #self.credentials = pika.PlainCredentials(self.username,self.password)
      self.credentials = pika.credentials.ExternalCredentials()
      ssl_options_dict = {
                          "certfile":  self.ssl_cert,
                          "keyfile":   self.ssl_key,
                          "ca_certs":  self.ssl_ca_certs,
                          "cert_reqs": ssl.CERT_REQUIRED,
                         }

                           
      #logger.debug(str(ssl_options_dict))
      # setup our connection parameters
      self.parameters = pika.ConnectionParameters(
                                                  host               = self.host,
                                                  port               = self.port,
                                                  virtual_host       = self.virtual_host,
                                                  credentials        = self.credentials,
                                                  socket_timeout     = self.socket_timeout,
                                                  ssl                = True,
                                                  ssl_options        = ssl_options_dict,
                                                 )

   def create_queue(self,name,routing_key):
      # declare a random queue which this job will use to receive messages
      # durable = survive reboots of the broker
      # exclusive = only current connection can access this queue
      # auto_delete = queue will be deleted after connection is closed
      self.channel.queue_declare(
                                 queue       = str(name),
                                 durable     = self.queue_is_durable,
                                 exclusive   = self.queue_is_exclusive,
                                 auto_delete = self.queue_is_auto_delete
                                )

      # now bind this queue to the exchange, using a routing key
      # any message submitted to the echange with the
      # routing key will appear on this queue
      self.channel.queue_bind(exchange=self.exchange_name,
                              queue=str(name),
                              routing_key=str(routing_key)
                             )

   def close(self):
      #self.channel.close()
      #self.connection.close()
      self.channel = None
      self.connection = None

   def send_msg(self,
                  message_body,
                  routing_key,
                  exchange_name = None,
                  message_headers = {},
                  priority = 0, # make message persistent
                  delivery_mode = 2, # default
               ):
      try:
         if exchange_name is None:
            exchange_name = self.exchange_name
         
         timestamp = time.time()

         # create the message properties
         properties = pika.BasicProperties(
                                           delivery_mode = delivery_mode,
                                           priority      = priority,
                                           timestamp     = timestamp,
                                           headers       = message_headers,
                                          )

         logger.debug("sending message body:\n" +  str(message_body))
         logger.debug('sending message to exchange: ' + self.exchange_name)
         logger.debug('sending message with routing key: ' + routing_key)

         self.channel.basic_publish(
                                    exchange         = exchange_name,
                                    routing_key      = routing_key,
                                    body             = message_body,
                                    properties       = properties,
                                   )
      except Exception,e:
         logger.exception('exception received while trying to send message')
         raise Exception('exception received while trying to send message' + str(e))
   
   def receive_msg(self,queue_name):
      # retrieve one message
      method, properties, body = self.channel.basic_get(queue=queue_name)
      return method,properties,body

   def purge_queue(self,queue_name):
      self.channel.queue_purge(queue = queue_name)

