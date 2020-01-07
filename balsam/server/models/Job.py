from django.db import models
from .special_fields import JSONField, ArrayField

class Job(models.Model):
    class Meta:
        unique_together = [['site', 'workdir']]

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
    labels = JSONField(
        '''
        Use like K8s selectors.
        A shallow dict of k:v string pairs
        Replace "workflow_filter"
        But also used for all CRUD operations
        # -l formula=H2O -l method__startswith=CC
        '''
        default=dict
    )
    app = models.ForeignKey('service.App', on_delete=models.CASCADE)
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
    # (protocol, source, destination, options)
    # destination defaults to "."; must fall under 'data'
    stage_in = models.JSONField()
    stage_out = models.JSONField()

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

    @property
    def num_ranks(self):
        return self.num_nodes * self.ranks_per_node
