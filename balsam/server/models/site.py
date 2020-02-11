from django.db import models, transaction
from django.contrib.postgres.fields import JSONField


class SiteManager(models.Manager):
    @transaction.atomic
    def create(self, owner, hostname, path):
        status = SiteStatus.objects.create()
        site = Site(owner=owner, hostname=hostname, path=path, status=status,)
        site.save()
        return site


class SiteStatus(models.Model):
    num_nodes = models.IntegerField(
        default=0, blank=True, help_text="Nodes visible to this Balsam site"
    )
    num_idle_nodes = models.IntegerField(
        default=0, blank=True, help_text="Number of idle nodes"
    )
    num_busy_nodes = models.IntegerField(
        default=0, blank=True, help_text="Number of nodes currently allocated"
    )
    backfill_windows = JSONField(
        default=list, blank=True, help_text="List of (num_nodes, time_minutes) tuples"
    )
    queued_jobs = JSONField(
        default=list, blank=True, help_text="List of jobs in the scheduler's queue"
    )

    @property
    def num_down_nodes(self):
        return self.num_nodes - self.num_idle_nodes - self.num_busy_nodes

    def update(self, **kwargs):
        keys = [
            "num_nodes",
            "num_idle_nodes",
            "num_busy_nodes",
            "backfill_windows",
            "queued_jobs",
        ]
        d = {k: kwargs[k] for k in keys if k in kwargs}
        self.__dict__.update(d)
        self.save()
        return self


class Site(models.Model):
    objects = SiteManager()

    class Meta:
        unique_together = [["hostname", "path"]]

    hostname = models.CharField(max_length=128, blank=False)
    path = models.CharField(
        max_length=256, blank=False, help_text="Absolute path to site directory"
    )
    last_refresh = models.DateTimeField(auto_now=True)
    creation_date = models.DateTimeField(auto_now_add=True)
    owner = models.ForeignKey(
        "User",
        related_name="sites",
        null=False,
        on_delete=models.CASCADE,
        editable=False,
    )
    status = models.OneToOneField(SiteStatus, on_delete=models.CASCADE,)

    @transaction.atomic
    def update(self, hostname=None, path=None, status=None):
        if hostname is not None:
            self.hostname = hostname
        if path is not None:
            self.path = path
        self.save()
        if status is not None:
            self.status.update(**status)
        return self

    def delete(self, *args, **kwargs):
        with transaction.atomic():
            super().delete(*args, **kwargs)
            self.status.delete()

    def __str__(self):
        return f"{self.hostname}:{self.path}"
