import balsam.models
Job = balsam.models.BalsamJob
AppDef = balsam.models.ApplicationDefinition

def print_history(jobs):
    for job in jobs:
        print(f'Job {job.name} [{job.job_id}]')
        print(f'------------------------------------------------')
        print(f'{job.state_history}\n')

def print_jobs(jobs, verbose):
    if not verbose:
        header = Job.get_header()
        print(header)
        print('-'*len(header))
        for job in jobs:
            print(job.get_line_string())
    else:
        for job in jobs:
            print(job)

def print_subtree(job, indent=1):
    def job_str(job): return f"{job.name:10} [{str(job.job_id)[:8]}]"
    print('|'*indent, end=' ')
    print(5*indent*' ', job_str(job))
    for job in job.get_children():
        print_subtree(job, indent+1)

def print_jobs_tree(jobs):
    roots = [j for j in jobs if j.parents=='[]']
    for job in roots: print_subtree(job)

def ls_jobs(namestr, show_history, jobid, verbose, tree, wf):
    results = Job.objects.all()
    if namestr: results = results.filter(name__icontains=namestr)
    if jobid: results = results.filter(job_id__icontains=jobid)
    if wf: results = results.filter(workflow__icontains=wf)
    
    if not results:
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
    else:
        header = AppDef.get_header()
        print(header)
        print('-'*len(header))
        for app in results:
            print(app.get_line_string())


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
