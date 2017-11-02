import sys,os,ssl
import pika,time

import logging
logger = logging.getLogger(__name__)
logging.getLogger('pika').setLevel(logging.WARNING)

from common import MessageInterface

class PikaMessageInterface(MessageInterface.MessageInterface):

    def __init__(self, settings):
        self.username                 = settings['username']
        self.password                 = settings['password']
        self.host                     = settings['host']
        self.port                     = settings['port']
        self.virtual_host             = settings['virtual_host']
        self.socket_timeout           = settings['socket_timeout']
        self.exchange_name            = settings['exchange_name']
        self.exchange_type            = settings['exchange_type']
        self.exchange_durable         = settings['exchange_durable']
        self.exchange_auto_delete     = settings['exchange_auto_delete']
        self.queue_name               = settings['queue_name']
        self.queue_is_durable         = settings['queue_is_durable']
        self.queue_is_exclusive       = settings['queue_is_exclusive']
        self.queue_is_auto_delete     = settings['queue_is_auto_delete']
        self.default_routing_key      = settings['default_routing_key']

        self.ssl_cert                 = settings['ssl_cert']
        self.ssl_key                  = settings['ssl_key']
        self.ssl_ca_certs             = settings['ssl_ca_certs']
        self.credentials = None
        self.parameters = None
        self.connection = None
        self.channel = None
        self.consume_msg = None

    def send_msg(self, message_body, routing_key=None):
        exchange_name = self.exchange_name
        message_headers = {}
        priority = 0
        delivery_mode = 2
        if routing_key is None:
            routing_key = self.default_routing_key
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

        try:
            self.channel.basic_publish(
                exchange         = exchange_name,
                routing_key      = routing_key,
                body             = message_body,
                properties       = properties,
            )
        except Exception as e:
            logger.exception('exception received while trying to send message')
            raise Exception('exception received while trying to send message' + str(e))

    def receive_msg(self):
        method, properties, body = self.channel.basic_get(queue=self.queue_name)
        return body

    def close(self):
        self.channel = None
        self.connection = None

    def setup_send(self):
        self._open_blocking_connection()

    def setup_receive(self, consume_msg=None, routing_key=None):
        if routing_key is None:
            self.routing_key = self.default_routing_key
        if consume_msg is not None:
            self.consume_msg = consume_msg

        # setup receiving queue and exchange
        logger.debug( ' open blocking connection to setup queue ' )
        self._open_blocking_connection()
        self._create_queue(self.queue_name,self.routing_key)
        self._close()

        logger.debug( ' open select connection ' )
        try:
            self._open_select_connection(self._on_connection_open)
        except:
            logger.exception(' Received exception while opening select connection: ' + str(sys.exc_info()[1]))
            raise

    def start_receive_loop(self):
        logger.debug( ' start message consumer ' )
        try:
            self.connection.ioloop.start()
        except:
            logger.exception(' Received exception while starting ioloop for message consumer: ' + str(sys.exc_info()[1]))
            raise

    def stop_receive_loop(self):
        try:
            logger.debug(' message connection: ' + str(self.connection) )
            logger.debug(' message ioloop: ' + str(self.connection.ioloop) )
            self.connection.ioloop.stop()
            logger.debug( ' after stopping message consumer ')
        except:
            logger.exception(' Received exception while stopping ioloop for the message consumer: ' + str(sys.exc_info()[1]))
            raise

    def _open_blocking_connection(self):
        logger.debug("open blocking connection")
        self._create_connection_parameters()

        try:
            self.connection = pika.BlockingConnection(self.parameters)
        except:
            logger.exception(' Exception received while trying to open blocking connection')
            raise
        try:
            self.channel   = self.connection.channel()
        except:
            logger.exception(' Exception received while trying to open a channel')
            raise
        logger.debug("create exchange, name = " + self.exchange_name) 
        self.channel.exchange_declare(
            exchange       = self.exchange_name,
            exchange_type  = self.exchange_type,
            durable        = self.exchange_durable,
            auto_delete    = self.exchange_auto_delete,
        )

    def _open_select_connection(self,
                                on_open_callback              = None,
                                on_open_error_callback        = None,
                                on_close_callback             = None,
                                stop_ioloop_on_close          = True,
                                ):
        logger.debug("create select connection")
        self._create_connection_parameters()
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
                logger.error(' Exception received while trying to open select connection' 
                             + str(sys.exc_info()))
                raise

    def _create_connection_parameters(self):
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

    def _create_queue(self,name,routing_key):
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

    def _purge_queue(self):
       self.channel.queue_purge(queue=self.queue_name)
      
    def _on_connection_open(self,connection):
        logger.debug(' in on_connection_open')
        try:
            connection.channel(self._on_channel_open)
        except:
            logger.exception(' Received exception while opening connection to message server: ' + str(sys.exc_info()[1]))
            raise
   
    def _on_channel_open(self,channel):
        logger.debug(' in on_channel_open')
        try:
            channel.basic_consume(self.consume_msg, self.queue_name)
        except:
            logger.exception(' Received exception while creating message consumer: ' + str(sys.exc_info()[1]))
            raise
