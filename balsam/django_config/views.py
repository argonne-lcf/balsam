#from django.http import HttpResponse
from django.shortcuts import render
from balsam.core.models import BalsamJob, ApplicationDefinition
import os


def index(request):
    jobs = BalsamJob.objects.all()
    apps = ApplicationDefinition.objects.all()

    env_name = 'NoEnv'
    if 'BALSAM_DB_PATH' in os.environ:
        env_name = os.path.basename(os.environ['BALSAM_DB_PATH'])

    if len(apps) == 0:
        status_text = '''        <p class="lead">You currently have no applications defined in your database, which is required before you can define jobs to run.</p>
        <p class="lead">
            <a href="/balsam/add_app/" class="btn btn-lg btn-secondary">Add An App</a>
        </p>'''
    elif len(jobs) == 0:
        status_text = f'''        <p class="lead">You currently have no jobs and {len(apps)} applications defined in your database. Click here to create your first job.</p>
        <p class="lead">
            <a href="/balsam/add_job.html" class="btn btn-lg btn-secondary">Add A Job</a>
        </p>'''
    else:
        status_text = ''

    context = {'n_jobs': len(jobs),
               'n_apps': len(apps),
               'env_name': env_name,
               'status_text': status_text}
    return render(request, 'index.html', context)


