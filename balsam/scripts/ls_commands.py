from balsam.service import models
from os.path import basename
import uuid
Job = models.BalsamJob
AppDef = models.ApplicationDefinition

def app_string(app, cmd=''):
    if not app:
        return " ".join(basename(p) for p in cmd.split())
    else:
        return " ".join(basename(p) for p in app.split())

def print_history(jobs):
    query = jobs.values_list('name', 'job_id', 'state_history')
    for (name,job_id,state_history) in query:
        print(f'Job {name} [{job_id}]')
        print(f'------------------------------------------------')
        print(f'{state_history}\n')

def print_jobs(jobs, verbose):
    if verbose:
        for job in jobs: print(job)
        return

    query = jobs.values_list('job_id', 'name', 'workflow', 'application', 'direct_command', 'state')
    apps = [app_string(job[3], job[4]) for job in query]
    jobs = [ (str(job[0]), job[1], job[2], app, job[5]) 
              for job,app in zip(query, apps) ]
    fields = ("job_id", "name", "workflow", "application", "state")

    widths = [max((len(job[field]) for job in jobs)) for field in range(5)]
    widths = [max(w,len(f)) for w,f in zip(widths, fields)]
    format = ' | '.join(f'%{width}s' for width in widths)
    header = format % fields

    print(header)
    print('-'*len(header))
    for job in jobs:
        print(format % tuple(f.ljust(w) for f,w in zip(job, widths)))

def print_subtree(job, indent=1):
    def job_str(job): return f"{job.name:10} {job.cute_id}"
    print('|'*indent, end=' ')
    print(5*indent*' ', job_str(job))
    for job in job.get_children():
        print_subtree(job, indent+1)

def print_jobs_tree(jobs):
    roots = [j for j in jobs if j.parents=='[]']
    for job in roots: print_subtree(job)

def ls_jobs(namestr, show_history, jobid, verbose, tree, wf, state):
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
    else: print_jobs(results, verbose)

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
    query = results.values_list(*fields)
    exes = [app_string(app[2]) for app in query]
    apps = [ (str(q[0]), q[1], exe, q[3]) for q,exe in zip(query, exes) ]

    widths = [max((len(app[field]) for app in apps)) for field in range(4)]
    widths = [max(w,len(f)) for w,f in zip(widths, fields)] 
    format = ' | '.join(f'%{width}s' for width in widths)
    header = format % fields

    print(header)
    print('-'*len(header))
    for app in apps:
        print(format % tuple(f.ljust(w) for f,w in zip(app, widths)))


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
