from io import StringIO
from traceback import print_exc
import json
import os
import uuid
import zmq

from django.db.utils import OperationalError
from concurrency.exceptions import RecordModifiedError

REQ_TIMEOUT = 10000 # 10 seconds
REQ_RETRY = 3


class Client:
    def __init__(self, server_info):
        import logging
        self.logger = logging.getLogger(__name__)
        self.server_info = server_info
        self.serverAddr = self.server_info.get('address')
        self.first_message = True
        if self.serverAddr:
            try:
                response = self.send_request('TEST_ALIVE', timeout=300)
            except:
                raise RuntimeError("Cannot reach server at {self.serverAddr}")
            else:
                if response != 'ACK':
                    self.logger.exception(f"sqlite client cannot reach DB write server")
                    raise RuntimeError("Cannot reach server at {self.serverAddr}")

    def send_request(self, msg, timeout=None):
        if timeout is None:
            timeout = REQ_TIMEOUT

        if self.first_message:
            self.first_message = False
            self.logger.debug(f"Connected to DB write server at {self.serverAddr}")

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
                client.close()
                poll.unregister(client)
                context.term()
                self.logger.debug(f"received reply: {reply}")
                return reply.decode('utf-8')
            else:
                self.logger.debug("No response from server, retrying...")
                client.setsockopt(zmq.LINGER, 0)
                client.close()
                poll.unregister(client)
                self.server_info.refresh()
                self.serverAddr = self.server_info['address']
                self.logger.debug(f"Connecting to DB write server at {self.serverAddr}")

        context.term()
        raise OperationalError(f"Sqlite client save request failed after " 
                               f"{REQ_RETRY} retries: is the server down?")

    def save(self, job, force_insert=False, force_update=False, using=None, update_fields=None):
        serial_data = job.serialize(force_insert=force_insert,
            force_update=force_update, using=using,
            update_fields=update_fields)

        self.logger.info(f"client: sending request for save of {job.cute_id}")
        response = self.send_request(serial_data)
        if response == 'ACK_RECORD_MODIFIED':
            raise RecordModifiedError(target=job)
        else:
            assert response.startswith('ACK_SAVE')
            job_id = uuid.UUID(response.split()[1])
            if job.job_id is None:
                job.job_id = job_id
            else:
                assert job.job_id == job_id
