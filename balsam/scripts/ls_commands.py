from balsam.core import models
from os import environ
from os.path import basename
import uuid
from django.core.exceptions import FieldError
Job = models.BalsamJob
AppDef = models.ApplicationDefinition
QueuedLaunch = models.QueuedLaunch

try: LIMIT = int(environ.get('BALSAM_LS_LIMIT', 5000))
except ValueError: LIMIT = 5000

def app_string(app):
    return " ".join(basename(p) for p in app.split())

def print_table(queryset, fields, field_header={}, transforms={}):
    fields = list(fields)
    num_fields = len(fields)
    records = list(queryset.values_list(*fields))

    # rename columns in field_header dict to the desired values
    for field_name, header_name in field_header.items():
        i = fields.index(field_name)
        fields[i] = header_name

    # apply transforms on each column
    transforms = {fields.index(field_name) : fxn for field_name,fxn in transforms.items()}
    for irow, row in enumerate(records):
        row = list(map(str, row))
        for i, fxn in transforms.items():
            row[i] = fxn(row[i])
        records[irow] = row

    widths = [max((len(row[field]) for row in records)) for field in range(num_fields)]
    widths = [max(w,len(f)) for w,f in zip(widths, fields)] 
    format = ' | '.join(f'%{width}s' for width in widths)
    header = format % tuple(fields)
    print(header)
    print('-'*len(header))
    for row in records:
        print(format % tuple(f.ljust(w) for f,w in zip(row, widths)))

def print_history(jobs):
    count = jobs.count()
    query = jobs[:LIMIT].values_list('name', 'job_id', 'state_history')
    for (name,job_id,state_history) in query:
        print(f'Job {name} [{job_id}]')
        print(f'------------------------------------------------')
        print(f'{state_history}\n')
    if count > LIMIT: print(f"({count-LIMIT} more BalsamJobs not shown...)")

def print_jobs(jobs, verbose):
    count = jobs.count()
    jobs = jobs[:LIMIT]
    if verbose:
        for job in jobs: print(job)
        if count > LIMIT: print(f"({count-LIMIT} more BalsamJobs not shown...)")
        return

    fields = ['job_id', 'name', 'workflow', 'application', 'state']
    add = environ.get('BALSAM_LS_FIELDS')
    if add: fields.extend([s.strip() for s in add.split(':') if s])
    transforms = {'application' : app_string}
    try:
        print_table(jobs, fields, transforms=transforms)
        if count > LIMIT: print(f"({count-LIMIT} more BalsamJobs not shown...)")
    except FieldError as e:
        print("***  You specified a nonexistant field in BALSAM_LS_FIELDS; please unset this ***")
        print(e)
        return

def print_by_states(jobs):
    from django.db.models import Count
    states = jobs.values('state').annotate(s_count=Count('state'))
    for result in states:
        state, count = result['state'], result['s_count']
        print(f"{state.rjust(16)}  {count}")

def print_subtree(job, indent=1):
    def job_str(job): return f"{job.name:10} {job.cute_id}"
    print('|'*indent, end=' ')
    print(5*indent*' ', job_str(job))
    for job in job.get_children():
        print_subtree(job, indent+1)

def print_jobs_tree(jobs):
    count = jobs.count()
    roots = jobs.filter(parents='[]')[:LIMIT]
    for job in roots: print_subtree(job)
    if count > LIMIT:
        print(f"(Omitted {count-LIMIT} jobs from display...)")

def ls_jobs(namestr, show_history, jobid, verbose, tree, wf, state, by_states):
    results = Job.objects.all()
    if namestr: results = results.filter(name__icontains=namestr)

    if jobid and len(jobid) < 36:
        results = results.filter(job_id__icontains=jobid)
    elif jobid:
        try:
            pk = uuid.UUID(jobid.strip())
            results = results.filter(job_id=pk)
        except ValueError:
            results = results.filter(job_id__icontains=jobid)

    if wf: results = results.filter(workflow__icontains=wf)
    if state: results = results.filter(state=state)
    
    if not results.exists():
        print("No jobs found matching query")
        return

    if show_history: print_history(results)
    elif tree: print_jobs_tree(results)
    elif by_states: print_by_states(results)
    else: print_jobs(results, verbose)

def ls_queues(verbose):
    from balsam.service.schedulers.exceptions import StatusNonZeroReturnCode
    try:
        QueuedLaunch.refresh_from_scheduler()
    except StatusNonZeroReturnCode as e:
        print(f"Scheduler is not reachable: {e}")
        return
    allq = QueuedLaunch.objects.all()
    if verbose:
        for q in allq: print(q)
        return
    if not allq.exists():
        print("No queued jobs detected")
        return

    fields = ['scheduler_id', 'project', 'queue', 'nodes', 'wall_minutes', 'state', 'job_mode', 'command', 'from_balsam']
    header = {'command' : 'Command', 'from_balsam' : 'Submitted from Balsam?'}
    transforms = {'Command' : lambda x: basename(x)}
    print_table(allq, fields, header, transforms)

def ls_apps(namestr, appid, verbose):
    if namestr:
        results = AppDef.objects.filter(name__icontains=namestr)
    elif appid:
        results = AppDef.objects.filter(job_id__icontains=appid)
    else:
        results = AppDef.objects.all()
    if not results:
        print("No apps found matching query")
        return
    if verbose:
        for app in results: print(app)
        return

    fields = ('pk', 'name', 'executable', 'description')
    transforms = {'executable' : app_string}
    print_table(results, fields, transforms=transforms)


def ls_wf(name, verbose, tree, wf):
    workflows = Job.objects.order_by().values('workflow').distinct()
    workflows = [wf['workflow'] for wf in workflows]

    if wf: name = wf # wf argument overrides name

    if name and name not in workflows:
        print(f"No workflow matching {name}")
        return

    if name and name in workflows: 
        workflows = [name]
        verbose = True

    print("Workflows")
    print("---------")
    for wf in workflows:
        print(wf)
        if tree:
            print('-'*len(wf))
            jobs_by_wf = Job.objects.filter(workflow=wf)
            print_jobs_tree(jobs_by_wf)
            print()
        elif verbose:
            print('-'*len(wf))
            jobs_by_wf = Job.objects.filter(workflow=wf)
            print_jobs(jobs_by_wf, False)
            print()
