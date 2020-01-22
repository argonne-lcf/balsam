import uuid
from django.db import models
from django.contrib.auth.models import AbstractUser, UserManager
from django.contrib.postgres.fields import JSONField

class BalsamUserManager(UserManager):
    pass

class User(AbstractUser):
    objects = BalsamUserManager()