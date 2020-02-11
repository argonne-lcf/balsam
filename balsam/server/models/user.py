from django.contrib.auth.models import AbstractUser, UserManager


class BalsamUserManager(UserManager):
    pass


class User(AbstractUser):
    objects = BalsamUserManager()
