from pathlib import Path
from urllib.parse import urlparse
from uuid import UUID
from django.db import models
from .exceptions import ValidationError

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

class TransferItemManager(models.Manager):

    def create(self, direction, source, destination, job):
        if direction not in ['in', 'out']:
            raise ValidationError(f'Parameter `direction` must be "in" or "out".')

        if direction == 'in':
            return self._create_stage_in(source, destination, job)
        return self._create_stage_out(source, destination, job)
    
    def _create_stage_in(self, source, destination, job):
        try:
            scheme, netloc, source_path = self._parse_url(source)
        except ValueError as e:
            raise ValidationError(f'Stage-in `source` URL is not formatted correctly: {e}')

        destination = Path(destination)
        if destination.is_absolute():
            raise ValidationError('Stage In `destination` must be a relative path to Job workdir')

        transfer = TransferItem(
            job=job, protocol=scheme, direction='in', remote_netloc=netloc,
            source_path=source_path, destination_path=destination.as_posix(),
        )
        transfer.save()
        return transfer
    
    def _create_stage_out(self, source, destination, job):
        try:
            scheme, netloc, destination_path = self._parse_url(destination)
        except ValueError as e:
            raise ValidationError(f'Stage-out `destination` URL is not formatted correctly: {e}')

        source = Path(source)
        if source.is_absolute():
            raise ValidationError('Stage Out `source` must be a relative path to Job workdir')

        transfer = TransferItem(
            job=job, protocol=scheme, direction='out', remote_netloc=netloc,
            source_path=source.as_posix(), destination_path=destination_path,
        )
        transfer.save()
        return transfer

    def _parse_url(self, url):
        o = urlparse(url)
        scheme, netloc, path = o.scheme, o.netloc, o.path
        allowed_schemes = [p[0] for p in TRANSFER_PROTOCOLS]
        if scheme not in allowed_schemes:
            raise ValueError(
                f'URL must start with `scheme://` where `scheme` is one of: {allowed_schemes}'
            )
        if scheme == 'globus':
            netloc = str(UUID(netloc))

        if not Path(path).is_absolute():
            raise ValueError(f'URL must end with an absolute path to a file or directory')

        return scheme, netloc, Path(path).as_posix()

class TransferItem(models.Model):
    '''
    TransferItems are created atomically as jobs
    are created. A job moves to STAGED_IN when its
    corresponding TransferItems are all done
    '''

    objects = TransferItemManager()
    
    protocol = models.CharField(max_length=16, editable=False, choices=TRANSFER_PROTOCOLS)
    state = models.CharField(max_length=32, default='pending', choices=TRANSFER_ITEM_STATES)
    direction = models.CharField(
        max_length=4,
        choices=[('in', 'Stage In'), ('out', 'Stage Out')]
    )
    remote_netloc = models.CharField(editable=False, max_length=256)
    source_path = models.CharField(editable=False, max_length=256)
    destination_path = models.CharField(editable=False, max_length=256)
    job = models.ForeignKey(
        'Job',
        related_name='transfer_items',
        editable=False,
        on_delete=models.CASCADE
    )
    task_id = models.CharField(blank=True, default='', max_length=32)
    status_message = models.TextField(blank=True, default='')

    def __repr__(self):
        if self.direction == 'in':
            src = f'{self.protocol}://{self.remote_netloc}{self.source_path}'
            dest = self.destination_path
            return f'TransferItem(direction="in", source="{src}", destination="{dest}")'
        src = self.source_path
        dest = f'{self.protocol}://{self.remote_netloc}{self.destination_path}'
        return f'TransferItem(direction="out", source="{src}", destination="{dest}")'

    def update(self, **kwargs):
        update_kwargs = ['state', 'status_message', 'task_id']
        for k, v in kwargs.items():
            if k not in update_kwargs:
                raise ValueError(f'Unexpected update kwarg {k}')
            if v is not None:
                setattr(self, k, v)
        self.save()