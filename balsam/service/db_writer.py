import json
import os
import logging
import zmq
from socket import gethostname
import signal

from django.conf import settings
from django.db.utils import OperationalError

logger = logging.getLogger('balsam.service.db_writer')
    
SOCKFILE_PATH = None
SOCKFILE_NAME = 'db_writer_socket'
PORT = "5556"
SERVER_PERIOD = 1000
CLIENT_TIMEOUT = 10000 # 10 seconds

class ZMQProxy:
    def __init__(self):
        import django
        os.environ['DJANGO_SETTINGS_MODULE'] = 'balsam.django_config.settings'
        django.setup()
        from balsam.service.models import BalsamJob
        self.BalsamJob = BalsamJob
        global SOCKFILE_PATH
        SOCKFILE_PATH = settings.INSTALL_PATH

        self.setup()

    def setup(self):
        hostname = gethostname()
        self.address = f'tcp://{hostname}:{PORT}'

        self.sock_file = os.path.join(SOCKFILE_PATH, SOCKFILE_NAME)

        with open(self.sock_file, 'w') as fp:
            fp.write(self.address)

        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(f'tcp://*:{PORT}')
        return self.socket

    def recv_request(self):
        events = self.socket.poll(timeout=SERVER_PERIOD)
        if events:
            message = self.socket.recv().decode('utf-8')
        else:
            message = None
        return message

    def send_reply(self, msg):
        self.socket.send_string(msg)

    def _django_save(self, job_msg):
        d = json.loads(job_msg)
        job = self.BalsamJob.from_dict(d)
        force_insert = d['force_insert']
        force_update = d['force_update']
        using = d['using']
        update_fields = d['update_fields']
        job._save_direct(force_insert, force_update, using, update_fields)
        logger.info(f"db_writer Saved {job.cute_id}")

class ZMQClient:
    def __init__(self):
        global SOCKFILE_PATH
        SOCKFILE_PATH = settings.INSTALL_PATH
        self.discover_zmq_proxy()

    def discover_zmq_proxy(self):
        path = os.path.join(SOCKFILE_PATH, SOCKFILE_NAME)
        if os.path.exists(path):
            self.zmq_server = open(path).read().strip()
        else:
            self.zmq_server = None
            return

        if 'tcp://' not in self.zmq_server: 
            self.zmq_server = None
            return

        response = self.send_request('TEST_ALIVE')
        if response == 'ACK':
            logger.info(f"save() going to server @ {self.zmq_server}")
        else:
            logger.info(f"save() going directly to local db")
            self.zmq_server = None

    def send_request(self, msg):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.setsockopt(zmq.LINGER, 2000)
        socket.connect(self.zmq_server)
        socket.send_string(msg)

        response = socket.poll(timeout=CLIENT_TIMEOUT)
        if response and response > 0:
            return socket.recv().decode('utf-8')
        else:
            return None

    def save(self, job, force_insert=False, force_update=False, using=None, update_fields=None):
        serial_data = job.serialize(force_insert=force_insert,
            force_update=force_update, using=using,
            update_fields=update_fields)

        response = self.send_request(serial_data)
        if response is None:
            raise OperationalError("ZMQ DB write request timed out")
        else:
            assert response == 'ACK_SAVE'

    def term_server(self):
        if self.zmq_server:
            response = self.send_request('TERM')

def server_main():
    parent_pid = os.getppid()

    handler = lambda a,b: 0
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)
    proxy = ZMQProxy()

    try:
        while True:
            message = proxy.recv_request()
            if message is None:
                if os.getppid() != parent_pid:
                    logger.info("db_writer detected parent PID died; quitting")
                    break
            elif 'job_id' in message:
                proxy._django_save(message)
                proxy.send_reply("ACK_SAVE")
            elif 'TERM' in message:
                logger.info("db_writer got TERM message; quitting")
                proxy.send_reply("ACK_TERM")
                break
            else:
                proxy.send_reply("ACK")
    finally:
        os.remove(os.path.join(SOCKFILE_PATH, SOCKFILE_NAME))

if __name__ == "__main__":
    server_main()
