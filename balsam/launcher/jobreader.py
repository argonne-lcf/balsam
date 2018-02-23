'''JobReaders are responsible for pulling relevant jobs from the Balsam database.
The base class provides a constructor that uses the command line arguments to
initialize the appropriate JobReader. It also contains a generic method for
filtering the Balsam Job database query (e.g. ignore jobs that are 
finished, ignore jobs with Applications that cannot run locally)
'''
from collections import defaultdict
from django.conf import settings
from balsam.service import models
BalsamJob = models.BalsamJob

import logging
import uuid
logger = logging.getLogger(__name__)


class JobReader():
    '''Define JobReader interface and provide generic constructor, filters'''

    WAITING_STATES = ['CREATED', 'LAUNCHER_QUEUED', 'AWAITING_PARENTS']
    RUNNABLE_STATES = ['PREPROCESSED', 'RESTART_READY']
    ALMOST_RUNNABLE_STATES = ['READY','STAGED_IN']

    @staticmethod
    def from_config(config):
        '''Constructor: build a FileJobReader or WFJobReader from argparse
        arguments

        If a job file is given, a FileJobReader will be constructed to read only
        BalsamJob primary keys from that file. Otherwise, a WFJobReader is
        created.
        
        Args:
            - ``config``:  command-line arguments *namespace* object returned by
              argparse.ArgumentParser
        Returns:
            - ``JobReader`` instance
        '''
        if config.job_file: return FileJobReader(config.job_file)
        else: return WFJobReader(config.wf_name)
    
    def by_states(self, states):
        '''Queryset of jobs matching one of states'''
        self.refresh_from_db()
        if isinstance(states, str):
            states = [states]
        elif isinstance(states, dict):
            states = states.keys()
        return self.jobs.filter(state__in=states)
    
    def refresh_from_db(self):
        '''Reload and re-filter jobs from the database'''
        jobs = self._get_jobs()
        jobs = self._filter(jobs)
        self.jobs = jobs

    def get_runnable(self, remaining_minutes, serial_only=False):
        runnable_jobs = self.by_states(self.RUNNABLE_STATES)
        runnable_jobs = runnable_jobs.filter(wall_time_minutes__lte=remaining_minutes)
        if serial_only:
            runnable_jobs = runnable_jobs.filter(num_nodes=1)
            runnable_jobs = runnable_jobs.filter(ranks_per_node=1)
        return runnable_jobs
   
    def _get_jobs(self): raise NotImplementedError
    
    def _filter(self, job_queryset):
        '''Filter out jobs that are done or cannot run locally'''
        jobs = job_queryset.exclude(state__in=models.END_STATES)
        jobs = jobs.filter(allowed_work_sites__icontains=settings.BALSAM_SITE)
        return jobs

    
class FileJobReader(JobReader):
    '''Read a file of whitespace delimited BalsamJob primary keys. Primarily
    intended for use by the Metascheduler to execute specific workloads.'''
    def __init__(self, job_file):
        self.jobs = []
        self.job_file = job_file
        self.pk_list = None
        logger.info(f"Taking jobs from file {self.job_file}")

    def _get_jobs(self):
        if self.pk_list is None:
            pk_strings = open(self.job_file).read().split()
            self.pk_list = [uuid.UUID(pk) for pk in pk_strings]
        jobs = BalsamJob.objects.filter(job_id__in=self.pk_list)
        return jobs


class WFJobReader(JobReader):
    '''Consume all jobs from DB, optionally matching a Workflow name.
    Will not consume jobs scheduled by metascheduler'''
    def __init__(self, wf_name):
        self.jobs = []
        self.wf_name = wf_name
        if wf_name: 
            logger.info(f"Consuming jobs from workflow {wf_name}")
        else:
            logger.info("Consuming all jobs from local DB")
    
    def _get_jobs(self):
        objects = BalsamJob.objects
        wf = self.wf_name
        jobs = objects.filter(workflow=wf) if wf else objects.all()
        jobs = jobs.filter(scheduler_id__exact='')
        return jobs
