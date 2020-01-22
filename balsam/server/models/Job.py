from django.db import models, transaction
from django.contrib.postgres.fields import JSONField, ArrayField
from django.utils import timezone
from datetime import timedelta
import uuid
import logging

logger = logging.getLogger(__name__)

ALLOWED_TRANSITIONS = {
    'CREATED': ('READY', 'KILLED')
}

class InvalidStateError(Exception): pass

class EventLogManager(models.Manager):
    def create(self, job, old_state, new_state, timestamp=None, message='', save=True):
        if timestamp is None:
            timestamp = timezone.now()

        log = EventLog(
            job=job,
            from_state=old_state,
            to_state=new_state,
            timestamp=timestamp,
            message=message
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
        lock = JobLock(
            site=site,
            label=label
        )
        lock.save()
        return lock

    def clear_stale(self):
        expiry_time = timezone.now() - self.EXPIRATION_PERIOD
        expired_locks = JobLock.objects.filter(heartbeat__lte=expiry_time)
        num_deleted, _ = expired_locks.delete()
        logger.info(f"Cleared {num_deleted} expired locks")

class JobLock(models.Model):
    objects = JobLockManager()

    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False
    )
    heartbeat = models.DateTimeField(auto_now=True)
    label = models.CharField(max_length=64)
    site = models.ForeignKey(
        'Site',
        on_delete = models.CASCADE
    )

    def tick(self):
        self.save(update_fields=['heartbeat'])

class JobManager(models.Manager):
    def create(self):
        pass

    def acquire_for_run(self, site):
        pass

    def acquire_for_processing(self, site):
        pass

    def release(self, site, pk_list):
        jobs = self.model.filter(site=site, pk__in=pk_list)
        jobs.update(lock=None)

    def bulk_update_state(self, patch_dict):
        logs = []
        qs = self.get_queryset()
        jobs = list(qs.filter(pk__in=patch_dict))
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


    def release_all(self, site):
        jobs = self.model.filter(site=site)
        jobs.update(lock=None)


class Job(models.Model):

    # Metadata
    workdir = models.CharField(
        '''
        Workdir *relative* to site data directory (cannot start with '/')
        For security, all jobs must execute inside the site data
        directory and transfers may only write into job workdirs.
        Further, all apps must be defined outside of this directory.
        This prevents overwriting a registered App with external code 
        ''',
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

    app_exchange = models.ForeignKey('AppExchange', on_delete=models.CASCADE)
    app_backend = models.ForeignKey(
        'AppBackend', on_delete=models.CASCADE,
        null=True
    )
    parameters = JSONField(default=dict)

    batch_job = models.ForeignKey('BatchJob', on_delete=models.SET_NULL, null=True, blank=True)
    state = models.CharField(max_length=64)
    last_update = models.DateTimeField(auto_now=True)
    data = JSONField(default=dict)
    
    # DAG: each Job can refer to 'parents' and 'children' attrs
    parents = models.ManyToManyField('self',
        verbose_name='Parent Jobs',
        blank=True,
        symmetrical=False,
        editable=False,
        related_name='children',
    )

    # Data movement
    # Job resources are created with zero or more TransferItems
    # The TransferItems contain (protocol, source, destination, options)

    # Resource Specification
    # We choose a concrete, over-simplified schema over a super flexible
    # JSON "resource spec" because while the latter might seem more 
    # future-proof, it makes launcher and platform implementations too
    # complex.
    
    num_nodes = models.IntegerField()
    ranks_per_node = models.IntegerField()
    threads_per_rank = models.IntegerField()
    threads_per_core = models.IntegerField()
    cpu_affinity = models.CharField(max_length=32)
    gpus_per_rank = models.IntegerField()
    node_packing_count = models.IntegerField()

    wall_time_min = models.IntegerField(
        'Minimum walltime necessary to run the job',
        default=0
    )

    # No environment vars: could set LD_PRELOAD
    # Envs defined at App level

    # Runtime methods like get_envs and app_cmd defined on local (Client) Job model
    # Here, instance methods only deal with state changes to the DB
    
    @property
    def site(self):
        if self.app_backend_id is not None:
            return self.app_backend.site

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
        return self, log