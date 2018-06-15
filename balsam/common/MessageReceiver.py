from balsam.common import PikaMessageInterface, NoMessageInterface
from django.conf import settings
import logging,sys,multiprocessing,time,os
logger = logging.getLogger(__name__)

RECEIVER_MAP = {
    'pika' : PikaMessageInterface.PikaMessageInterface,
    'no_message' : NoMessageInterface.NoMessageInterface,
    'zmq' : ZMQMessageInterface.ZMQMessageInterface
}

class MessageReceiver(multiprocessing.Process):
    ''' subscribes to a queue and executes the given callback'''
    
    def __init__(self, settings):
        # execute multiprocessing.Process superconstructor
        super(MessageReceiver,self).__init__()

        receiver_mode = settings['mode']
        MessageClass = RECEIVER_MAP[receiver_mode]
        self.messageInterface = MessageClass(settings)
        self.consume_msg = getattr(self, '%s_consume_msg' % receiver_mode)

    def handle_msg(self, msg_body):
        '''This handles the message in a protocol-independent way'''
        raise NotImplementedError
   
    def run(self):
       logger.debug(' in run ')
       self.messageInterface.setup_receive(self.consume_msg)
       self.messageInterface.start_receive_loop()

    def pika_consume_msg(self,channel,method_frame,header_frame,body):
       logger.debug('in pika_consume_msg' )
       if body is not None:
           logger.debug(' received message: ' + body )
           try:
               self.handle_msg(body)
           except Exception as e:
               logger.exception('failed to handle_msg. not continuing with this job')
               channel.basic_ack(method_frame.delivery_tag)
               return
       else:
           logger.error(' consume_msg called, but body is None ')
           # should be some failure notice to argo
           # acknowledge receipt of message
           channel.basic_ack(method_frame.delivery_tag)

    def no_message_consume_msg(self):
       pass

   def zmq_consume_msg(self, body):
       logger.debug(' in zmq_message_consume_msg')
       if body:
           logger.debug(' received ZMQmessage: ' + body)
           try:
               self.handle_msg(body)
            except Exception as e:
                logger.exception('failed to handle_msg. not continuing with this job')
                return
        else:
            logger.error(' consume_msg called, but body is empty or None'))

    def shutdown(self):
       logger.debug(' stopping message consumer ')
       self.messageInterface.stop_receive_loop()
