import json
import os
import logging
import zmq
from socket import gethostname

import django
from django.conf import settings
os.environ['DJANGO_SETTINGS_MODULE'] = 'balsam.django_config.settings'
django.setup()

from balsam.service.models import BalsamJob
INSTALL_PATH = settings.INSTALL_PATH
logger = logging.getLogger('balsam.service')

def setup():
    hostname = gethostname()
    port = "5556"

    with open(os.path.join(INSTALL_PATH, 'db_writer_socket'), 'w') as fp:
        fp.write(f'tcp://{hostname}:{port}')

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(f'tcp://*:{port}')
    return socket

def save_job(job_msg):
    d = json.loads(job_msg)
    force_insert = d['force_insert']
    force_update = d['force_update']
    using = d['using']
    update_fields = d['update_fields']
    job = BalsamJob.from_dict(d)
    job._save_direct(force_insert, force_update, using, update_fields)
    logger.info(f"db_writer Saved {job.cute_id}")

def main():
    socket = setup()
    try:
        while True:
            message = socket.recv().decode('utf-8')
            if 'job_id' in message:
                save_job(message)
            socket.send_string("ACK")
    finally:
        os.remove(os.path.join(INSTALL_PATH, 'db_writer_socket'))

if __name__ == "__main__":
    main()
