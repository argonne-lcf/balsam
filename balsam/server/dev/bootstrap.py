from django import setup
import os
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'balsam.server.conf.settings')
setup()
from balsam.server.models import *

misha = User.objects.create_user(username='misha', email='f@f.net', password='f', is_staff=True, is_superuser=True)
mysite = Site.objects.create(owner=misha, hostname='localhost', path='/foo')

hello = AppExchange.objects.create(
    name="say-hello",
    description="echos hello to a name",
    parameters=['name'],
    site=mysite,
    class_name='Demo.HelloApp',
    owner=misha
)