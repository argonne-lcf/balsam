from django import setup
import os
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'balsam.server.conf.settings')
setup()
from balsam.server.models import *

misha = User.objects.create_user(username='misha', email='f@f.net', password='f', is_staff=True, is_superuser=True)
mysite = Site.objects.create(owner=misha, hostname='localhost', path='/foo')

backend = [{"site": mysite, "class_name": "Demo.SayHello"}]

hello = AppExchange.objects.create_new(
    name="say-hello",
    description="echos hello {name}",
    parameters=['name'],
    backend_dicts=backend,
    owner=misha,
)