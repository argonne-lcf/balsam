# These statements must come before any other imports
#import django
import os
#os.environ['DJANGO_SETTINGS_MODULE'] = 'argobalsam.settings'
#django.setup()
# --------------------
from django.conf import settings
import balsam.models

BalsamJob = balsam.models.BalsamJob

def newapp(args):
    pass

def newjob(args):
    pass

def newdep(args):
    pass

def ls(args):
    pass

def modify(args):
    pass

def rm(args):
    pass

def qsub(args):
    pass

def kill(args):
    pass

def mkchild(args):
    pass

def launcher(args):
    pass

def service(args):
    pass
