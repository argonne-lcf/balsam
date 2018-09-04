from django.conf import settings
import configparser
import json
import os
from collections import namedtuple
import logging
import pprint
logger = logging.getLogger('balsam.service')

Queue = namedtuple('Queue', ['name', 'min_time', 'max_time'])

class _QueuePolicy:
    def __init__(self):
        self.queues = {}
        self.max_nodes = 0
        self.min_nodes = 0
        
        top = settings.BALSAM_HOME
        policy_fname = settings.QUEUE_POLICY
        policypath = os.path.join(top, policy_fname)
        config = configparser.ConfigParser()
        config.read(policypath)
        for queue_name in config.sections(): 
            qconf = config[queue_name]
            self.add_from_config(queue_name, qconf)
        msg = pprint.pformat(self.queues, indent=4)
        logger.info('Loaded Queue Policy\n'+msg)

    def add_from_config(self, queue_name, qconf):
        global_max_nodes = self.max_nodes
        global_min_nodes = self.min_nodes
        try:
            submit_jobs = qconf.getboolean('submit-jobs')
            max_queued = qconf.getint('max-queued')
            qpolicy = json.loads(qconf['policy'])
        except Exception as e:
            logger.error(f'Failed to parse {queue_name}: {e}')
            return
        if not submit_jobs or max_queued==0: return
        try:
            q = {'max_queued' : max_queued}
            for rule in qpolicy:
                min_nodes = int(rule['min-nodes'])
                max_nodes = int(rule['max-nodes'])
                assert min_nodes <= max_nodes
                assert (min_nodes, max_nodes) not in q
                global_max_nodes = max(global_max_nodes, max_nodes)
                global_min_nodes = min(global_min_nodes, min_nodes)
                min_time = float(rule['min-time'])
                max_time = float(rule['max-time'])
                assert min_time <= max_time
                q[(min_nodes, max_nodes)] = (min_time, max_time)
        except Exception as e:
            logger.error(f'Failed to parse policy for {queue_name}: {e}')
        else:
            self.queues[queue_name] = q
            self.max_nodes = global_max_nodes
            self.min_nodes = global_min_nodes

    def find_queue(self, open_queues, num_nodes):
        for qname in open_queues:
            queue = self.queues[qname]
            for node_range, time_range in queue.items():
                if node_range == 'max_queued': continue
                low, high = node_range
                if low <= num_nodes <= high:
                    min_time, max_time = time_range
                    return Queue(name, min_time, max_time)

_queue_policy = _QueuePolicy()
max_nodes = _queue_policy.max_nodes
min_nodes = _queue_policy.min_nodes
queues = _queue_policy.queues
find_queue = _queue_policy.find_queue
