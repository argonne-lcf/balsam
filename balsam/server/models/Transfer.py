from django.db import models

TRANSFER_PROTOCOLS = (
    ('globus', 'Globus Online'),
    ('scp', 'scp'),
    ('rsync', 'rsync'),
    ('cp', 'cp')
)

TRANSFER_ITEM_STATES = (
    ('pending', 'Pending Transfer'), 
    ('active', 'In Progress'),
    ('finished', 'Transfer Finished'), 
    ('failed', 'Transfer Failed'),
)

class TransferItem(models.Model):
    '''
    TransferItems are created atomically as jobs
    are created. A job moves to STAGED_IN when its
    corresponding TransferItems are all done
    '''
    
    # destination defaults to "."; must fall under job workdir
    protocol = models.CharField(max_length=16, choices=TRANSFER_PROTOCOLS)
    state = models.CharField(max_length=32, choices=TRANSFER_ITEM_STATES)
    direction = models.CharField(
        max_length=4, 
        choices=[('in', 'Stage In'), ('out', 'Stage Out')]
    )
    source = models.CharField(max_length=256)
    destination = models.CharField(max_length=256)
    job = models.ForeignKey(
        'Job',
        related_name='transfer_items',
        on_delete=models.CASCADE
    )
    task_id = models.CharField(blank=True, default='', max_length=32)
    status_message = models.TextField(blank=True)