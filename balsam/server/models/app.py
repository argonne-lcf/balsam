"""
Only App name is stored in DB
The app command and associated scripts are stored locally in the site
Timeout and Error handling behaviors also defined simply on the App
"""
from django.db import models, transaction
from django.contrib.postgres.fields import JSONField
from .exceptions import ValidationError


class AppManager(models.Manager):
    def create(self, **kwargs):
        return self.create_new(**kwargs)

    @transaction.atomic
    def create_new(self, name, description, parameters, backend_dicts, owner, users=[]):
        if self.get_queryset().filter(owner=owner, name=name).exists():
            raise ValidationError(
                f"User {owner.username} already has an App named {name}"
            )

        backends = []
        for data in backend_dicts:
            b, _ = AppBackend.objects.get_or_create(
                site=data["site"], class_name=data["class_name"]
            )
            backends.append(b)

        app = AppExchange(
            name=name, description=description, parameters=parameters, owner=owner,
        )
        app.save()
        app.backends.set(set(backends))

        user_set = set(users)
        user_set.add(owner)
        app.users.set(user_set)
        return app

    @transaction.atomic
    def create_merged(self, name, existing_apps, owner, description=None):
        if self.get_queryset().filter(owner=owner, name=name).exists():
            raise ValidationError(
                f"User {owner.username} already has an App named {name}"
            )

        parameter_tuples = [tuple(app.parameters) for app in existing_apps]
        if len(set(parameter_tuples)) != 1:
            raise ValidationError(f"Cannot merge apps with different parameters")
        parameters = parameter_tuples[0]

        if any(app.owner != owner for app in existing_apps):
            raise ValidationError(f"Merged apps must all have same owner")

        if description is None:
            description = max((app.description for app in existing_apps), key=len)

        exchange = AppExchange(
            name=name, description=description, parameters=parameters, owner=owner
        )
        exchange.save()
        backends = [b for app in existing_apps for b in app.backends.all()]
        exchange.backends.set(set(backends))
        users = [u for app in existing_apps for u in app.users.all()]
        users.append(owner)
        exchange.users.set(set(users))
        return exchange


class AppBackend(models.Model):
    class Meta:
        unique_together = [["site", "class_name"]]

    site = models.ForeignKey(
        "Site",
        related_name="registered_app_backends",
        null=False,
        editable=False,
        on_delete=models.CASCADE,
    )
    class_name = models.CharField(
        max_length=128, help_text="The app defined at {AppModule}.{AppClass}"
    )
    exchanges = models.ManyToManyField(
        "AppExchange", related_name="backends", blank=True,
    )

    @property
    def owner(self):
        return self.site.owner

    def __str__(self):
        return self.class_name


class AppExchange(models.Model):
    class Meta:
        unique_together = [["name", "owner"]]

    objects = AppManager()
    name = models.CharField(max_length=128, blank=False)
    description = models.TextField(blank=True, default="")
    parameters = JSONField(
        default=list,
        help_text="""
        List of parameters that must provided for each Job""",
    )
    owner = models.ForeignKey(
        "User", on_delete=models.CASCADE, related_name="owned_apps"
    )
    users = models.ManyToManyField("User", related_name="apps", blank=True,)

    def update_backends(self, backend_dicts):
        new_backends = []
        for data in backend_dicts:
            b, _ = AppBackend.objects.get_or_create(
                site=data["site"], class_name=data["class_name"]
            )
            new_backends.append(b)

        self.backends.set(set(new_backends))
        AppBackend.objects.filter(exchanges=None).delete()

    @transaction.atomic
    def update(
        self,
        name=None,
        description=None,
        parameters=None,
        users=None,
        backend_dicts=None,
    ):
        if name is not None:
            self.name = name
        if description is not None:
            self.description = description
        if parameters is not None:
            self.parameters = parameters
        if users is not None:
            user_set = set(users)
            user_set.add(self.owner_id)
            self.users.set(user_set)
        self.save()
        if backend_dicts is not None:
            self.update_backends(backend_dicts)

    def delete(self, *args, **kwargs):
        with transaction.atomic():
            super().delete(*args, **kwargs)
            AppBackend.objects.filter(exchanges=None).delete()
