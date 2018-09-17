from getpass import getuser
import os
from socket import gethostname

from django.shortcuts import render, redirect
from balsam.core.models import BalsamJob

def home_page(requests):
    return redirect('/balsam/tasks')

def info_str():
    user = getuser()
    host = gethostname()
    db_name = os.path.basename(os.environ.get('BALSAM_DB_PATH'))
    return f"<p>{user}@{host}</p>Balsam DB: {db_name}"

def list_tasks(request):
    info = info_str()
    return render(request, 'tasks.html', {'db_info':info})

def list_apps(request):
    info = info_str()
    return render(request, 'apps.html', {'db_info':info})
