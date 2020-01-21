from django.db import models
from django.contrib.postgres.fields import JSONField, ArrayField

STATE_CHOICES = ('pending-submission', 'queued', 'starting', 'running', 'exiting', 'finished', 'dep_hold', 'user_hold')
STATE_CHOICES = [(s,s) for s in STATE_CHOICES]

class BatchJobManager(models.Manager):

    def create(self, scheduler_id, project, queue, num_nodes, wall_time_min, state, **kwargs):
        batch_job = self.model(
            scheduler_id=job_id,
            project=job_spec['project'],
            queue=job_spec['queue'],
            num_nodes=job_spec['num_nodes'],
            wall_time_min=job_spec['wall_time_min'],
            state=job['state']
        )
        batch_job.save()
        return batch_job

    def bulk_update_state(self, scheduler_ids, state):
        jobs = self.model.filter(site=site, scheduler_id__in=scheduler_ids)
        jobs.update(state=state)

    def bulk_refresh(self, site, status_dicts):
        jobs = self.model.filter(site=site)
        existing_ids = set(jobs.values_list('scheduler_id', flat=True))

        for job_id, job_spec in status_dicts.items():
            if job_id not in existing_ids:
                self.create(**job_spec)
            else:
                batch_job = self.models.get(scheduler_id=job_id)
                batch_job.update(**job_spec)
                existing_ids.remove(job_id)

        if existing_ids:
            self.bulk_update_state(existing_ids, 'finished')


class BatchJob(models.Model):
    objects = BatchJobManager()

    scheduler_id = models.IntegerField(db_index=True, unique=True)
    project = models.CharField(max_length=128)
    queue = models.CharField(max_length=128)
    num_nodes = models.IntegerField()
    wall_time_min = models.IntegerField()
    job_mode = models.CharField(max_length=32)
    filter_tags = JSONField(default=dict)
    state = models.CharField(
        max_length=32,
        default='pending-submission', choices=STATE_CHOICES
    )
    site = models.ForeignKey(
        'Site',
        on_delete=models.CASCADE
    )
    start_time = models.DateTimeField()
    end_time = models.DateTimeField()
