import pika
import sys
import time
import datetime
import json

import logging
from django.conf import settings
from balsam_core.MessageInterface import MessageInterface

class NoMoreMessages(Exception): pass

credentials = pika.PlainCredentials(settings.BALSAM_PIKA_USERNAME, settings.BALSAM_PIKA_PASSWORD)
parameters = pika.ConnectionParameters(
    host=settings.BALSAM_PIKA_HOSTNAME,
    port=settings.BALSAM_PIKA_PORT,
    virtual_host='/',
    credentials=credentials,
    socket_timeout=120)

def get_job_to_estimate():
    global credentials, parameters
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    queue_name = '%s.getjobs' % settings.BALSAM_SITE
    method_frame, header_frame, body = channel.basic_get(queue=queue_name)
    if method_frame:
        # print method_frame
        # print header_frame
        # print body
        channel.basic_ack(method_frame.delivery_tag)
    else:
        raise NoMoreMessages('No message returned')

    # Cancel the consumer and return any pending messages
    requeued_messages = channel.cancel()
    # print 'Requeued %i messages' % requeued_messages

    # Close the channel and the connection
    channel.close()
    connection.close()

    b = eval(body)
    return b

def get_jobs_to_estimate():
    urls = []
    try:
        while 1:
            urls.append( get_job_to_estimate() )
    except NoMoreMessages:
        #print "No more messages"
        pass

    return urls

def get_job_to_submit():
    global credentials, parameters
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    queue_name = '%s.submit' % settings.BALSAM_SITE
    method_frame, header_frame, body = channel.basic_get(queue=queue_name)
    if method_frame:
        # print method_frame
        # print header_frame
        # print body
        channel.basic_ack(method_frame.delivery_tag)
    else:
        raise NoMoreMessages('No message returned')

    # Cancel the consumer and return any pending messages
    requeued_messages = channel.cancel()
    # print 'Requeued %i messages' % requeued_messages

    # Close the channel and the connection
    channel.close()
    connection.close()

    b = eval(body)
    return b

def get_jobs_to_submit():
    urls = []
    try:
        while 1:
            urls.append( get_job_to_submit() )
    except NoMoreMessages:
        #print "No more messages"
        pass

    return urls

def send_message(job_id, operation, message='dummy message'):
    global credentials, parameters

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    exchange_name = 'hpc'
    hpc_name = settings.BALSAM_SITE
    taskid_str = str(job_id)
    routing_key = '%s.%s.%s' % (settings.BALSAM_SITE, str(job_id), operation)
    timestamp = time.time()

    headers = { # example how headers can be used
        'hpc': hpc_name,
        'taskID':taskid_str,
        'operation':operation,
        'created': int(timestamp)
        }
    data = { # example hot to transfer objects rather than string using json.dumps and json.loads
        'hpc': hpc_name,
        'taskID':taskid_str,
        'operation':operation,
        'created': int(timestamp),
        'message': message
        }

    properties=pika.BasicProperties(
        delivery_mode=2, # makes persistent job
        priority=0, # default priority
        timestamp=timestamp, # timestamp of job creation
        headers=headers )

    channel.basic_publish(exchange=exchange_name,
                          routing_key=routing_key,
                          body=json.dumps(data), # must be a string
                          properties=properties)
    print " [x] Sent %r:%r" % (routing_key, message)
    connection.close()

def send_job_estimate(job_id, estimate):
    send_message(job_id, 'jobestimate', estimate)

def send_job_submit(job_id):
    send_message(job_id, 'submit','http://atlasgridftp02.hep.anl.gov:40000/hpc/jobs/testjob.xml')

def send_job_failed(job_id, message):
    send_message(job_id, 'finishedjob', 'Error: ' + message)

def send_job_finished(job_id):
    send_message(job_id, 'finishedjob', 'Success')

if __name__ == '__main__':
    print get_jobs_to_estimate()
    print get_jobs_to_submit()


