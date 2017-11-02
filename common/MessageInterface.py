import logging
logger = logging.getLogger(__name__)

class MessageInterface:
    '''These are the public methods to be implemented by MessageInterfaces like
    PikaMessageInterface.  All protocol-specfic methods should be hidden'''
   
    def __init__(self, settings):
        raise NotImplementedError

    def setup_send(self):
        raise NotImplementedError
    
    def setup_receive(self, consume_msg=None):
        raise NotImplementedError
    
    def send_msg(self, message_body):
        raise NotImplementedError

    def receive_msg(self):
        raise NotImplementedError

    def start_receive_loop(self):
        raise NotImplementedError
    
    def stop_receive_loop(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError
