import logging
logger = logging.getLogger(__name__)

import asyncio
import zmq
import time
from zmq.asyncio import Context

class ZMQMessageInterface(MessageInterface.MessageInterface):
    def __init__(self, settings):
        zmq.asyncio.install()

        self.ctx = zmq.asyncio.Context()
        self.sock_sub = None
        self.sock_pub = None

        self.default_routing_key = b''
        self.host = 'tcp://127.0.0.1'
        self.port = 5555

    def setup_send(self):
        self.sock_pub = self.ctx.socket(zmq.PUB)
        self.sock_pub.bind('%s:%d' % (self.host, self.port))
        time.sleep(1)

    def setup_receive(self, consume_msg=None, routing_key=None):
        if routing_key is None:
            self.routing_key = self.default_routing_key
        if consume_msg is not None:
            self.consume_msg = consume_msg

        self.sock_sub = self.ctx.socket(zmq.SUB)
        self.sock_sub.connect('%s:%d' % (self.host, self.port))
        self.sock_sub.subscribe(self.routing_key)
        time.sleep(1)

    def send_msg(self, message_body, routing_key=None):
        if routing_key is None:
            routing_key = self.default_routing_key
        if isinstance(message_body, str):
            message_body = message_body.encode('utf-8')
        self.sock_pub.send(message_body)

    def receive_msg(self):
        msg = self.sock_sub.recv_multipart()
        body = ''.join(s.decode('utf-8') for s in msg)
        return body

    def start_receive_loop(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._recv_loop())
        loop.close()

    async def _recv_loop(self):
        while True:
            msg = await self.sock_sub.recv_multipart()
            body = ''.join(s.decode('utf-8') for s in msg)
            self.consume_msg(body)

    def stop_receive_loop(self):
        pass

    def close(self):
        if self.sock_sub is not None:
            self.sock_sub.close()
            self.sock_sub = None
        if self.sock_pub is not None:
            self.sock_pub.close()
            self.sock_pub = None
