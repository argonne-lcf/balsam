from getpass import getuser
import os
from socket import gethostname

from django.shortcuts import render, redirect
from balsam.core.models import BalsamJob

def info_str():
    user = getuser()
    host = gethostname()
    db_name = os.path.basename(os.environ.get('BALSAM_DB_PATH'))
    return f"<p>{user}@{host}</p> {db_name}"

def list_tasks(request):
    info = info_str()
    _jobs = BalsamJob.objects.values_list('job_id', 'name', 'workflow', 'state')
    return render(request, 'tasks.html', {'db_info':info, 'tasks':_jobs})

def list_apps(request):
    info = info_str()
    return render(request, 'apps.html', {'db_info':info})

def foo(request):
    if request.is_ajax():
        print("Got AJAX Request!")
        print("Method:", request.method)
        print("Raw data:", request.body)
    return HttpResponse(f"<div>OK</div><div>{request.body}</div>")
