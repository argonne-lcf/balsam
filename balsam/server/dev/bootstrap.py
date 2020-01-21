from django import setup
import os
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'balsam.server.conf.settings')
setup()
from balsam.server.models import *

misha = User.objects.create_user(username='misha', email='f@f.net', password='f')
mysite = Site.objects.create(owner=misha, hostname='localhost', site_path='/foo')
myapp = App.objects.create(site=mysite, name='foo')
myjob = Job.objects.create(
    app=myapp,
    site=mysite,
    num_nodes=1,
    ranks_per_node=1,
    threads_per_rank=1,
    threads_per_core=1,
    gpus_per_rank=1,
    node_packing_count=1
)