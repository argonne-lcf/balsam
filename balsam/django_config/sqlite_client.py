from io import StringIO
from traceback import print_exc
import json
import os
import logging
import zmq

from django.db.utils import OperationalError

REQ_TIMEOUT = 10000 # 10 seconds
REQ_RETRY = 3


class Client:
    def __init__(self, server_info):
        self.logger = logging.getLogger(__name__)
        self.server_info = server_info
        self.serverAddr = self.server_info.get('address')
        if self.serverAddr:
            self.logger.debug(f"trying to reach DB write server at {self.serverAddr}")
            response = self.send_request('TEST_ALIVE', timeout=3)
            if response != 'ACK':
                self.logger.exception(f"sqlite client cannot reach DB write server")
                raise RuntimeError("Cannot reach DB write server")

    def send_request(self, msg, timeout=None):
        if timeout is None:
            timeout = REQ_TIMEOUT

        context = zmq.Context(1)
        poll = zmq.Poller()
        
        for retry in range(REQ_RETRY):
            client = context.socket(zmq.REQ)
            client.connect(self.serverAddr)
            poll.register(client, zmq.POLLIN)

            client.send_string(msg)
            socks = dict(poll.poll(timeout))

            if socks.get(client) == zmq.POLLIN:
                reply = client.recv()
                context.term()
                return reply.decode('utf-8')
            else:
                self.logger.debug("No response from server, retrying...")
                client.setsockopt(zmq.LINGER, 0)
                client.close()
                poll.unregister(client)
                self.server_info.refresh()
                self.serverAddr = self.server_info['address']

        context.term()
        raise OperationalError(f"Sqlite client save request failed after " 
                               f"{REQ_RETRY} retries: is the server down?")

    def save(self, job, force_insert=False, force_update=False, using=None, update_fields=None):
        serial_data = job.serialize(force_insert=force_insert,
            force_update=force_update, using=using,
            update_fields=update_fields)

        self.logger.info(f"client: sending request for save of {job.cute_id}")
        response = self.send_request(serial_data)
        assert response == 'ACK_SAVE'
