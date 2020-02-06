from django.db import models, transaction
from django.contrib.postgres.fields import JSONField, ArrayField
from django.utils import timezone
from datetime import timedelta
import uuid
import logging
from .exceptions import InvalidStateError

logger = logging.getLogger(__name__)

ALLOWED_TRANSITIONS = {
    'CREATING': ('CREATED',),
    'CREATED': ('AWAITING_PARENTS', 'READY', 'STAGED_IN', 'PARENTS_FAILED',),
    'RESET': ('AWAITING_PARENTS', 'READY', 'STAGED_IN', 'PARENTS_FAILED',),
    'AWAITING_PARENTS': ('READY', 'PARENTS_FAILED',),
    'READY': ('STAGING_IN', 'STAGED_IN',),
    'STAGING_IN': ('STAGED_IN', 'FAILED',),
    'STAGED_IN': ('PREPROCESSED', 'FAILED',),
    'PREPROCESSED': ('RUN_READY',),
    'RUN_READY': ('RUNNING',),
    'RUNNING': ('RUN_DONE', 'RUN_ERROR', 'RUN_TIMEOUT', 'KILLED',),
    'RUN_DONE': ('POSTPROCESSED', 'RUN_READY', 'FAILED',),
    'RUN_ERROR': ('POSTPROCESSED', 'RUN_READY', 'FAILED',),
    'RUN_TIMEOUT': ('POSTPROCESSED', 'RUN_READY', 'FAILED',),
    'POSTPROCESSED': ('STAGING_OUT', 'STAGED_OUT', 'FINISHED',),
    'STAGING_OUT': ('STAGED_OUT', 'FAILED',),
    'STAGED_OUT': ('FINISHED',),
    'FINISHED': ('RESET',),
    'FAILED': ('RESET',),
    'KILLED': ('RESET',),
}
STATE_CHOICES = [
    (k, k.capitalize().replace('_', ' ')) for k in ALLOWED_TRANSITIONS
]

class EventLogManager(models.Manager):
    def create(self, job, old_state, new_state, timestamp=None, message='', save=True):
        if timestamp is None:
            timestamp = timezone.now()
        log = self.model(
            job=job, from_state=old_state, to_state=new_state,
            timestamp=timestamp, message=message
        )
        if save:
            log.save()
        return log

class EventLog(models.Model):
    objects = EventLogManager()
    job = models.ForeignKey(
        'Job',
        related_name='events',
        on_delete=models.CASCADE,
    )
    from_state = models.CharField(max_length=64)
    to_state = models.CharField(max_length=64)
    timestamp = models.DateTimeField(auto_now=False)
    message = models.TextField()

class JobLockManager(models.Manager):
    EXPIRATION_PERIOD = timedelta(minutes=3)
    def create(self, site, label):
        lock = JobLock(site=site, label=label)
        lock.save()
        return lock

    def clear_stale(self):
        expiry_time = timezone.now() - self.EXPIRATION_PERIOD
        qs = self.get_queryset()
        expired_locks = qs.filter(heartbeat__lte=expiry_time)
        num_deleted, _ = expired_locks.delete()
        logger.info(f"Cleared {num_deleted} expired locks")

class JobLock(models.Model):
    objects = JobLockManager()
    heartbeat = models.DateTimeField(auto_now=True)
    label = models.CharField(max_length=64)
    site = models.ForeignKey('Site', on_delete=models.CASCADE)
    
    def tick(self):
        self.save(update_fields=['heartbeat'])

    def release(self):
        logger.info(f"Released lock {self.pk}")
        self.delete()

class JobManager(models.Manager):
    def get_list_queryset(self):
        """Prefetch related items that would be hit in a big list view"""
        qs = self.get_queryset()
        qs = qs.prefetch_related('app_exchange', 'parents', 'app_backend', 'app_backend__site')
        return qs

    # TODO: validate that params match App requirements on create/update
    def validate_parameters(self, jobs):
        """
        On create, we need to fetch the existing Apps first
        On update, we can just prefetch-related Apps and access them
        """
        app_pks = set(job['app_exchange'] for job in jobs)
        params_map = {}
        for app in AppExchange.objects.filter(pk__in=app_pks):
            if app.pk not in params_map:
                params_map[app.pk] = set(app.parameters)
        for job in validated_data:
            required_params = params_map[job["app_exchange"]]
            diff = required_params.difference(job["parameters"].keys())
            if diff:
                raise ValidationError(f'Missing required parameters: {diff}')

    def bulk_create(self, job_list):
        app_set = set(job["app"] for job in job_list)
        job = self.model(state='CREATED')
        if job.app_exchange.backends.count() == 1:
            job.app_backend = job.app_exchange.backends.first()
        job.save()
        return job

    def create(self):
        pass

    def reset(self):
        pass

    def set_initial_state(self, reset=False):
        pass

    def acquire(self, site, lock):
        pass

    @transaction.atomic
    def bulk_update_state(self, patch_dict):
        logs = []
        qs = self.get_queryset()
        jobs = qs.filter(pk__in=patch_dict).select_for_update()

        if any(p['state'] in ['FINISHED', 'FAILED'] for p in patch_dict):
            jobs = jobs.prefetch_related('children')

        for job in jobs:
            event = job.update_state(
                **patch_dict[job.pk],
                save=False
            )
            logs.append(event)
        EventLog.objects.bulk_create(logs)
        Job.objects.bulk_update(
            jobs,
            update_fields=['state']
        )

class Job(models.Model):

    # Metadata
    workdir = models.CharField(
        '''Workdir *relative* to site data directory (cannot start with '/')''',
        max_length=256,
    )
    tags = JSONField(
        '''
        Use like K8s selectors.
        A shallow dict of k:v string pairs
        Replace "workflow_filter"
        But also used for all CRUD operations
        # -t formula=H2O -t method__startswith=CC''',
        default=dict
    )
    lock = models.ForeignKey(
        'JobLock',
        null=True,
        blank=True,
        default=None,
        on_delete=models.SET_NULL,
        db_index=True
    )
    owner = models.ForeignKey(
        'User',
        null=False,
        editable=False,
        on_delete=models.CASCADE,
        related_name='jobs'
    )

    app_exchange = models.ForeignKey(
        'AppExchange', related_name='jobs',
        on_delete=models.CASCADE
    )
    app_backend = models.ForeignKey(
        'AppBackend', on_delete=models.CASCADE,
        null=True, editable=False, related_name='jobs'
    )
    parameters = JSONField(default=dict)

    batch_job = models.ForeignKey(
        'BatchJob', on_delete=models.SET_NULL, 
        related_name='balsam_jobs', null=True, blank=True
    )
    state = models.CharField(
        max_length=32, default='CREATING', editable=False, db_index=True,
        choices=STATE_CHOICES
    )
    last_update = models.DateTimeField(auto_now=True)
    data = JSONField(default=dict)
    application_return_code = models.IntegerField(blank=True, null=True)
    last_message = models.TextField(blank=True, default='')
    
    # DAG: each Job can refer to 'parents' and 'children' attrs
    parents = models.ManyToManyField('self',
        verbose_name='Parent Jobs',
        blank=True,
        symmetrical=False,
        editable=False,
        related_name='children',
    )

    # Resource Specification
    num_nodes = models.IntegerField(default=1)
    ranks_per_node = models.IntegerField(default=1)
    threads_per_rank = models.IntegerField(default=1)
    threads_per_core = models.IntegerField(default=1)
    cpu_affinity = models.CharField(max_length=32, default='depth')
    gpus_per_rank = models.IntegerField(default=0)
    node_packing_count = models.IntegerField(default=1)
    wall_time_min = models.IntegerField(default=0)

    @property
    def site(self):
        # TODO: accessing this in large list-view will need prefetch_related!
        if self.app_backend_id is not None:
            return str(self.app_backend.site)
        return 'Pending Assignment'
    
    @property
    def app_class(self):
        # TODO: accessing this in large list-view will need prefetch_related!
        if self.app_backend_id is not None:
            return self.app_backend.class_name
        return 'Pending Assignment'

    def update_state(self, new_state, message='', timestamp=None, save=True):
        if new_state not in ALLOWED_TRANSITIONS[self.state]:
            raise InvalidStateError(f"Cannot transition from {self.state} to {new_state}")

        self.state = new_state
        log = EventLog.objects.create(
            job=self,
            old_state=self.state,
            new_state=new_state,
            timestamp=timestamp,
            message=message,
            save=False
        )
        if save:
            with transaction.atomic():
                log.save()
                self.save(update_fields=['state'])
        return log