from django.db import models
class TransferItem(models.Model):
    '''
    TransferItems are created atomically as jobs
    are created. A job moves to STAGED_IN when its
    corresponding TransferItems are all done
    '''
    state
    site
    movement_direction #in or out
    protocol
    source_path
    dest_path
    job # foreign key to BalsamJob
    transfer_task # foreign key to TransferTask

class TransferTask(models.Model):
