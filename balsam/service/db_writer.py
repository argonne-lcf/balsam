from io import StringIO
from traceback import print_exc
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
        logger.info(f"db_writer proxy listening at {self.address}")
        logger.info(f"db_writer address written to {self.sock_file}")
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
        if self.zmq_server is not None:
            logger.info(f"save() going to server @ {self.zmq_server}")
        else:
            logger.info(f"No db_writer detected; save() going directly to local db")

    def discover_zmq_proxy(self):
        path = os.path.join(SOCKFILE_PATH, SOCKFILE_NAME)
        if os.path.exists(path):
            self.zmq_server = open(path).read().strip()
            logger.debug(f"client discover: {self.zmq_server}")
        else:
            logger.debug(f"client discover: no db_socket_file exists")
            self.zmq_server = None
            return

        if 'tcp://' not in self.zmq_server: 
            logger.debug(f"client discover: invalid address")
            self.zmq_server = None
            return

        logger.debug(f"client discover: sending request TEST_ALIVE")
        response = self.send_request('TEST_ALIVE')
        if response != 'ACK':
            self.zmq_server = None
            logger.debug(f"client discover: no response; dead server")
        else:
            logger.debug(f"client discover: the server is alive!")

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

        logger.info(f"client: sending request for save of {job.cute_id}")
        response = self.send_request(serial_data)
        if response is None:
            raise OperationalError("ZMQ DB write request timed out")
        else:
            assert response == 'ACK_SAVE'

    def term_server(self):
        if self.zmq_server:
            response = self.send_request('TERM')

def server_main():
    logger.debug("hello from server_main")
    parent_pid = os.getppid()

    handler = lambda a,b: 0
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)
    logger.debug("making zmq proxy class")
    proxy = ZMQProxy()

    try:
        logger.info("db_writer starting up")
        while True:
            logger.info(f"proxy waiting for message")
            message = proxy.recv_request()
            logger.info(f"proxy received message")
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
    except:
        buf = StringIO()
        print_exc(file=buf)
        logger.exception(f"db_writer Uncaught exception:\n%s", buf.getvalue())
    finally:
        logger.info("exiting server main; deleting sock_file now")
        os.remove(os.path.join(SOCKFILE_PATH, SOCKFILE_NAME))

if __name__ == "__main__":
    server_main()
