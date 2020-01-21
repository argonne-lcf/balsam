from django.db import models, transaction
from django.contrib.postgres.fields import JSONField

class SiteManager(models.Manager):

    def create(self, owner, hostname, site_path):
        status = SiteStatus.objects.create()
        policy = SitePolicy.objects.create()
        site = Site(
            owner=owner, 
            hostname=hostname, 
            site_path=site_path,
            status=status,
            policy=policy,
            active=True
        )
        site.save()
        return site

class SiteStatus(models.Model):

    num_nodes = models.IntegerField(default=0, help_text="Nodes visible to this Balsam site")
    num_idle_nodes = models.IntegerField(default=0, help_text="Number of idle nodes")
    num_busy_nodes = models.IntegerField(default=0, help_text="Number of nodes currently allocated")
    backfill_windows = JSONField(default=list, help_text="List of (num_nodes, time_minutes) tuples")
    queued_jobs = JSONField(default=list, help_text="List of jobs in the scheduler's queue")
    reservations = JSONField(default=list, help_text="List of reservations on the system")

    @property
    def num_down_nodes(self):
        return self.num_nodes - self.num_idle_nodes - self.num_busy_nodes

class SitePolicy(models.Model):
    submission_mode = models.CharField(max_length=32)
    max_num_queued = models.IntegerField(default=0)
    min_num_nodes = models.IntegerField(default=0)
    max_num_nodes = models.IntegerField(default=0)
    min_wall_time_min = models.IntegerField(default=0)
    max_wall_time_min = models.IntegerField(default=0)
    max_total_node_hours = models.IntegerField(default=0)
    node_hours_used = models.IntegerField(default=0)

class Site(models.Model):
    objects = SiteManager()

    class Meta:
        unique_together = [['hostname', 'site_path']]

    hostname = models.CharField(max_length=128, blank=False, editable=False)
    site_path = models.CharField(max_length=256, blank=False, editable=False, help_text='root of data dir')
    active = models.BooleanField(default=True, editable=False, db_index=True)
    heartbeat = models.DateTimeField(auto_now=True)
    globus_endpoint = models.UUIDField(blank=True, null=True)
    owner = models.ForeignKey(
        'User',
        related_name='sites',
        null=False,
        on_delete=models.CASCADE,
        editable=False,
    )
    status = models.OneToOneField(
        SiteStatus,
        on_delete=models.CASCADE,
    )
    policy = models.OneToOneField(
        SitePolicy,
        on_delete=models.CASCADE,
    )

    @transaction.atomic
    def deactivate(self):
        self.active = False
        self.save()

    def update(self, **kwargs):
        fields = ['hostname', 'site_path']
        update_fields = [f for f in fields if f in kwargs]
        for f in update_fields:
            value = kwargs[f]
            setattr(self, f, kwargs[f])
        if update_fields:
            self.save(update_fields=update_fields)
        return self

    def __str__(self):
        return f'{self.hostname}:{self.site_path}'
