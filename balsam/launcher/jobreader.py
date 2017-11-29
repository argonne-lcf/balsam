from collections import defaultdict
import balsam.models
from balsam.models import BalsamJob

class JobReader():
    '''Interface with BalsamJob DB & pull relevant jobs'''
    @staticmethod
    def from_config(config):
        '''Constructor'''
        if config.job_file: return FileJobReader(config.job_file)
        else: return WFJobReader(config.wf_name)
    
    @property
    def by_states(self):
        '''dict of jobs keyed by state'''
        result = defaultdict(list)
        for job in self.jobs:
            result[job.state].append(job)
        return result
    
    def refresh_from_db(self):
        '''caller invokes this to read from DB'''
        jobs = self._get_jobs()
        jobs = self._filter(jobs)
        self.jobs = jobs
   
    def _get_jobs(self): raise NotImplementedError
    
    def _filter(self, job_queryset):
        jobs = job_queryset.exclude(state__in=balsam.models.END_STATES)
        jobs = jobs.filter(allowed_work_sites__icontains=settings.BALSAM_SITE)
        return jobs

    
class FileJobReader(JobReader):
    '''Limit to job PKs specified in a file. Used by metascheduler.'''
    def __init__(self, job_file):
        self.jobs = []
        self.job_file = job_file
        self.pk_list = None

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
    
    def _get_jobs(self):
        objects = BalsamJob.objects
        wf = self.wf_name
        jobs = objects.filter(workflow=wf) if wf else objects.all()
        jobs = jobs.filter(scheduler_id__exact='')
        return jobs
