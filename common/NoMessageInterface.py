import logging
import time
import threading
logger = logging.getLogger(__name__)

from common import MessageInterface

class NoMessageInterface(MessageInterface.MessageInterface):

    def __init__(self, settings):
        self.alive = True
        self.thread = None

    def setup_send(self):
        pass
    
    def setup_receive(self, consume_msg=None):
        self.thread = threading.Thread(target=self._fake_ioloop)
    
    def send_msg(self, message_body):
        pass

    def receive_msg(self):
        pass

    def start_receive_loop(self):
        try:
            logger.debug('starting dummy receiver ioloop')
            self.thread.start()
            self.thread.join()
        except Exception:
            logger.exception('failed to start dummy receiver ioloop')
    
    def stop_receive_loop(self):
        self.alive = False
        self.thread.join()

    def close(self):
        pass
    
    def _fake_ioloop(self):
        logger.debug('Thread: inside fake_ioloop')
        try:
            while self.alive:
                logger.debug('  Thread: Idling Receiver')
                time.sleep(15)
        except Exception:
            logger.debug('The ioloop failed')
