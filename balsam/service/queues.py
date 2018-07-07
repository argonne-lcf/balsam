from django.conf import settings
import configparser
import json
from collections import namedtuple

class LaunchNotQueued(Exception): pass

QRule = namedtuple('QRule', ['min_nodes', 'max_nodes', 'min_time', 'max_time'])

class JobQueue:

    def __init__(self, queue_name, submit_jobs, max_queued):
        self.name = queue_name
        self.submit_jobs = submit_jobs
        self.max_queued = max_queued
        self.rules = []
        self.jobs = []

    def __repr__(self):
        return f'{self.name} submit:{self.submit_jobs} maxQueued:{self.max_queued}'

    @property
    def num_queueable(self):
        return self.max_queued - len(self.jobs)

    def addRule(self, min_nodes, max_nodes, min_time, max_time):
        self.rules.append(QRule(min_nodes, max_nodes, min_time, max_time))

    def status_update(self):
        pass

    def submit(self, slaunch):
        pass

class QueueManager:

    def __init__(self):
        top = settings.BALSAM_HOME
        policy_fname = settings.QUEUE_POLICY
        policypath = os.path.join(top, policy_fname)
        config = configparser.ConfigParser()
        config.read(policypath)

        self.queues = {}
        
        for queue_name in config.sections():
            qconf = config[queue_name]
            self.add_from_config(queue_name, qconf)

    def __getitem__(self, key):
        return self.queues[key]

    def __iter__(self):
        return self.queues.values()

    @property
    def names(self):
        return list(self.queues.keys())

    @property
    def jobqueues(self):
        return list(self.queues.values())

    @property
    def num_queued(self):
        return sum(len(q) for q in self.jobqueues)

    def add_from_config(self, queue_name, qconf):
        try:
            submit_jobs = qconf.getBoolean('submit-jobs')
            max_queued = qconf.getInt('max-queued')
            qpolicy = json.loads(qconf['policy'])
        except Exception as e:
            logger.error(f'Failed to parse {queue_name}: {e}')
            return
        try:
            q = JobQueue(queue_name, submit_jobs, max_queued)
            for rule in qpolicy:
                min_nodes = rule['min-nodes']
                max_nodes = rule['max-nodes']
                min_time = rule['min-time']
                max_time = rule['max-time']
                q.addRule(min_nodes, max_nodes, min_time, max_time)
        except Exception as e:
            logger.error(f'Failed to parse policy for {queue_name}: {e}')
        else:
            self.queues[queue_name] = q

    def refresh(self):
        queuedict = scheduler.jobs_byqueue()
        for qname in queuedict:
            if qname in self.names: 
                jobs = queuedict[qname]
                self.queues[queue].jobs = jobs
