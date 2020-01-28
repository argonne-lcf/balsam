from django.db import models, transaction
from django.contrib.postgres.fields import JSONField, ArrayField

STATE_CHOICES = (
    'pending-submission', 'submit-failed', 'queued', 'starting',
    'running', 'exiting', 'finished', 'dep-hold', 'user-hold',
    'pending-deletion'
)
WAITING_STATES = ('pending-submission', 'queued', 'dep-hold', 'user-hold')
TERMINAL_STATES = ('submit-failed', 'finished')

STATE_CHOICES = [(s,s) for s in STATE_CHOICES]
JOB_MODE_CHOICES = ('mpi', 'serial', 'script')
JOB_MODE_CHOICES = [(s,s) for s in JOB_MODE_CHOICES]

class BatchJobQuerySet(models.QuerySet):
    def active_jobs(self):
        return self.exclude(state__in=TERMINAL_STATES)

class BatchJobManager(models.Manager):

    def get_queryset(self):
        return BatchJobQuerySet(self.model, using=self._db)

    def active_jobs(self):
        return self.get_queryset().active_jobs()

    def create(
        self, site, project, queue, num_nodes, wall_time_min,
        job_mode, filter_tags
    ):
        batch_job = self.model(
            site=site,
            scheduler_id=None,
            project=project,
            queue=queue,
            num_nodes=num_nodes,
            wall_time_min=wall_time_min,
            job_mode=job_mode,
            filter_tags=filter_tags,
            state="pending-submission",
        )
        batch_job.save()
        return batch_job

    @transaction.atomic
    def bulk_update(self, patch_list):
        """
        Args:
            patch_list: list of BatchJob {field: value} dicts
            site: The updating Balsam site
        """
        patch_map = {}
        for patch in patch_list:
            pk = patch.pop("pk")
            patch_map[pk] = patch

        jobs = self.model.filter(pk__in=patch_map)
        for job in jobs.select_for_update():
            patch = patch_map[job.pk]
            job.update(bulk_select_for_update=True, **patch)


class BatchJob(models.Model):
    objects = BatchJobManager()
    
    site = models.ForeignKey(
        'Site',
        on_delete=models.CASCADE,
        related_name='batchjobs',
    )
    scheduler_id = models.IntegerField(
        default=None, null=True, blank=True,
        help_text="Defaults to None until assigned by the site's scheduler",
    )
    project = models.CharField(max_length=128)
    queue = models.CharField(max_length=128)
    num_nodes = models.IntegerField(blank=False)
    wall_time_min = models.IntegerField(blank=False)
    job_mode = models.CharField(max_length=32, choices=JOB_MODE_CHOICES)
    filter_tags = JSONField(default=dict, blank=True)
    state = models.CharField(
        max_length=32,
        default='pending-submission', choices=STATE_CHOICES
    )
    status_message = models.TextField(blank=True, default='')
    start_time = models.DateTimeField(default=None, null=True, blank=True)
    end_time = models.DateTimeField(default=None, null=True, blank=True)

    def update_state(self, new_state):
        if self.state in TERMINAL_STATES:
            return
        if self.state == 'pending-deletion':
            if new_state != 'finished':
                return
        self.state = new_state

    def lock_and_refresh(self):
        locked_self = BatchJob.objects.select_for_update().get(pk=self.pk)
        self.__dict__.update(locked_self.__dict__)

    @transaction.atomic
    def update(self, bulk_select_for_update=False, **kwargs):
        pre_run_fields = [
            'scheduler_id', 'project', 'queue',
            'num_nodes', 'wall_time_min', 'job_mode', 'filter_tags'
        ]
        anytime_fields = ['status_message', 'start_time', 'end_time']

        if not bulk_select_for_update:
            self.lock_and_refresh()

        # These can only change while the job is still queued
        pre_run_fields = [f for f in pre_run_fields if f in kwargs]
        if self.state in WAITING_STATES:
            for field in pre_run_fields:
                setattr(self, field, kwargs[field])
        elif pre_run_fields:
            raise ValueError(f"{pre_run_fields} can only be updated while a job is queued")

        new_state = kwargs.pop('state', None)
        if new_state is not None:
            self.update_state(new_state)
        
        anytime_fields = [f for f in anytime_fields if f in kwargs]
        for field in anytime_fields:
            setattr(self, field, kwargs[field])
        self.save()