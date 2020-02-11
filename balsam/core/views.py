from getpass import getuser
import os,json
from socket import gethostname

from django.shortcuts import render, redirect
from balsam.core.forms import AddAppForm, AddBalsamJobForm
from balsam.core.models import BalsamJob, ApplicationDefinition


def home_page(requests):
    return redirect('/balsam/tasks')


def info_str():
    user = getuser()
    host = gethostname()
    db_name = os.path.basename(os.environ.get('BALSAM_DB_PATH'))
    return f"<p>{user}@{host}</p>Balsam DB: {db_name}"


def list_jobs(request):
    env_name = 'NoEnv'
    if 'BALSAM_DB_PATH' in os.environ:
        env_name = os.path.basename(os.environ['BALSAM_DB_PATH'])

    if request.method == 'POST':
        print(request)
        print(request.POST)

        print(dir(request.POST))
        if 'new_job' in request.POST:
            return redirect('balsam/add_job.html')

    jobs = BalsamJob.objects.all()
    return render(request, 'jobs.html', {'jobs': jobs, 'env_name': env_name})



def list_apps(request):
    env_name = 'NoEnv'
    if 'BALSAM_DB_PATH' in os.environ:
        env_name = os.path.basename(os.environ['BALSAM_DB_PATH'])
    apps = ApplicationDefinition.objects.all()
    return render(request, 'apps.html', {'apps': apps, 'env_name': env_name})


def add_app(request):
    env_name = 'NoEnv'
    if 'BALSAM_DB_PATH' in os.environ:
        env_name = os.path.basename(os.environ['BALSAM_DB_PATH'])

    context = {'env_name': env_name}

    # if this is a POST request we need to process the form data
    if request.method == 'POST':
        # create a form instance and populate it with data from the request:
        form = AddAppForm(request.POST)
        # check whether it's valid:
        if form.is_valid():
            form.save()
            return redirect('apps')
        else:
            for field in form.cleaned_data.keys():
                form.fields[field].initial = form.cleaned_data[field]
            context['is_valid'] = False
    # if a GET (or any other method) we'll create a blank form
    else:
        form = AddAppForm()

    context['form'] = form
    return render(request, 'add_app.html', context)


def edit_app(request, app_id):
    env_name = 'NoEnv'
    if 'BALSAM_DB_PATH' in os.environ:
        env_name = os.path.basename(os.environ['BALSAM_DB_PATH'])

    context = {'env_name': env_name}

    app = ApplicationDefinition.objects.get(id=app_id)

    # if this is a POST request we need to process the form data
    if request.method == 'POST':
        # create a form instance and populate it with data from the request:
        form = AddAppForm(request.POST)
        # check whether it's valid:
        if form.is_valid():
            for field in form.cleaned_data.keys():
                setattr(app, field, form.cleaned_data[field])
            app.save()
            return redirect('apps')

    # if a GET (or any other method) we'll create a form and populate it
    else:
        form = AddAppForm()
        # populate form with job attributes
        for field in form.fields:
            form.fields[field].initial = getattr(app, field)

    context['form'] = form
    context['app_id'] = app_id
    return render(request, 'edit_app.html', context)


def add_job(request):
    env_name = 'NoEnv'
    if 'BALSAM_DB_PATH' in os.environ:
        env_name = os.path.basename(os.environ['BALSAM_DB_PATH'])

    context = {'env_name': env_name}

    # if this is a POST request we need to process the form data
    if request.method == 'POST':
        print(request.POST)
        # if this POST came from a click on the "jobs" page
        # need to just create a new form and move on
        if request.POST.get('new_job'):
            form = AddBalsamJobForm()

            # check if there were selected jobs for dependencies
            if 'job_select' in request.POST:
                print(type(request.POST))
                print(dir(request.POST))
                lists = request.POST.lists()
                selected_job_ids = [ x for x in lists if 'job_select' in x[0]]
                selected_job_ids = selected_job_ids[0][1]
                print(selected_job_ids)
                form.fields['parents'].initial = json.dumps(selected_job_ids)
        # otherwise, we need to parse the submitted job
        else:
            # create a form instance and populate it with data from the request
            form = AddBalsamJobForm(request.POST)
            # check whether it's valid:
            if form.is_valid():
                form.save()
                return redirect('jobs')
            else:
                for field in form.cleaned_data.keys():
                    form.fields[field].initial = form.cleaned_data[field]

    # if a GET (or any other method) we'll create a blank form
    else:
        form = AddBalsamJobForm()

    context['form'] = form
    return render(request, 'add_job.html', context)


def edit_job(request, job_id):
    env_name = 'NoEnv'
    if 'BALSAM_DB_PATH' in os.environ:
        env_name = os.path.basename(os.environ['BALSAM_DB_PATH'])

    context = {'env_name': env_name}

    job = BalsamJob.objects.get(pk=job_id)

    # if this is a POST request we need to process the form data
    if request.method == 'POST':
        # create a form instance and populate it with data from the request:
        form = AddBalsamJobForm(request.POST)
        # check whether it's valid:
        if form.is_valid():
            for field in form.cleaned_data.keys():
                setattr(job, field, form.cleaned_data[field])
            job.save()
            return redirect('jobs')

    # if a GET (or any other method) we'll create a form and populate it
    else:
        form = AddBalsamJobForm()
        # populate form with job attributes
        for field in form.fields:
            form.fields[field].initial = getattr(job, field)

    context['form'] = form
    context['job_id'] = job_id
    return render(request, 'edit_job.html', context)
