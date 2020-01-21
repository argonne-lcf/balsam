"""
Only App name is stored in DB
The app command and associated scripts are stored locally in the site
Timeout and Error handling behaviors also defined simply on the App
"""
from django.db import models
from django.contrib.postgres.fields import JSONField, ArrayField

class AppManager(models.Manager):
    def create(self, name, description, parameters, site, class_name, owner):

        if not isinstance(parameters, list):
            raise ValueError(f"Expected list; got {type(parameters)}")

        if not all(isinstance(s, str) for s in parameters):
            raise ValueError(f"parameters can only contain str values")

        backend, _ = AppBackend.objects.get_or_create(
            site=site,
            class_name=class_name
        )
        app = AppExchange(
            name = name,
            description = description,
            parameters = parameters,
            owner = owner
        )
        app.save()
        app.backends.set(backend)
        return app

class AppBackend(models.Model):
    class Meta:
        unique_together = [['site', 'class_name']]

    site = models.ForeignKey(
        'Site',
        related_name='apps',
        null=False,
        editable=False,
        on_delete=models.CASCADE
    )
    class_name = models.CharField(
        max_length=128,
        help_text='The app defined at {AppModule}.{AppClass}'
    )
    exchanges = models.ManyToManyField(
        'AppExchange',
        related_name='backends',
    )

    @property
    def owner(self):
        return self.site.owner

class AppExchange(models.Model):
    class Meta:
        unique_together = [['name', 'owner']]
    name = models.CharField(max_length=128)
    description = models.TextField()
    parameters = JSONField(
        default=list,
        help_text='''
        List of parameters that must provided for each Job'''
    )
    owner = models.ForeignKey(
        'User',
        on_delete=models.CASCADE,
        related_name='owned_apps'
    )
    users = models.ManyToManyField(
        'User',
        related_name='apps'
    )

    def update(self, **kwargs):
        if 'users' in kwargs:
            group = set(kwargs['users'])
            group.add(self.owner_id)
            self.authorized_users.set(group)
