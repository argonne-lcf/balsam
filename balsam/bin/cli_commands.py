import os
from django.conf import settings
import balsam.models
from balsam import dag
import ls_commands as lscmd

Job = balsam.models.BalsamJob
AppDef = balsam.models.ApplicationDefinition

def cmd_confirmation(message=''):
    confirm = ''
    while not confirm.lower() in ['y', 'n']:
        try:
            confirm = input(f"{message} [y/n]: ")
        except: pass
    return confirm.lower() == 'y'

def newapp(args):
    if AppDef.objects.filter(name=args.name).exists():
        raise RuntimeError(f"An application named {args.name} exists")
    if not os.path.exists(args.executable):
        raise RuntimeError(f"Executable {args.executable} not found")
    if args.preprocess and not os.path.exists(args.preprocess):
        raise RuntimeError(f"Script {args.preprocess} not found")
    if args.postprocess and not os.path.exists(args.postprocess):
        raise RuntimeError(f"Script {args.postprocess} not found")

    app = AppDef()
    app.name = args.name
    app.description = ' '.join(args.description)
    app.executable = args.executable
    app.default_preprocess = args.preprocess
    app.default_postprocess = args.postprocess
    app.environ_vars = ":".join(args.env)
    app.save()
    print(app)
    print("Added app to database")


def newjob(args):
    if not AppDef.objects.filter(name=args.application).exists():
        raise RuntimeError(f"App {args.application} not registered in local DB")

    job = Job()
    job.name = args.name
    job.description = ' '.join(args.description)
    job.workflow = args.workflow
    job.allowed_work_sites = ' '.join(args.allowed_site)

    job.wall_time_minutes = args.wall_minutes
    job.num_nodes = args.num_nodes
    job.processes_per_node = args.processes_per_node
    job.threads_per_rank = args.threads_per_rank
    job.threads_per_core = args.threads_per_core

    job.application = args.application
    job.application_args = ' '.join(args.args)
    job.preprocess = args.preprocessor
    job.postprocess = args.postprocessor
    job.post_error_handler = args.post_handle_error
    job.post_timeout_handler = args.post_handle_timeout
    job.auto_timeout_retry = not args.disable_auto_timeout_retry
    job.input_files = ' '.join(args.input_files)

    job.stage_in_url = args.url_in
    job.stage_out_url = args.url_out
    job.stage_out_files = ' '.join(args.stage_out_files)
    job.environ_vars = ":".join(args.env)

    print(job)
    if not args.yes:
        if not cmd_confirmation('Confirm adding job to DB'):
            print("Add job aborted")
            return
    job.save()
    return job
    print("Added job to database")


def match_uniq_job(s):
    job = Job.objects.filter(job_id__icontains=s)
    if job.count() > 1:
        raise ValueError(f"More than one ID matched {s}")
    elif job.count() == 1: return job
    
    job = Job.objects.filter(name__contains=s)
    if job.count() > 1: job = Job.objects.filter(name=s)
    if job.count() > 1: 
        raise ValueError(f"More than one Job name matches {s}")
    elif job.count() == 1: return job

    raise ValueError(f"No job in local DB matched {s}")

def newdep(args):
    parent = match_uniq_job(args.parent)
    child = match_uniq_job(args.child)
    dag.add_dependency(parent, child)
    print(f"Created link [{str(parent.first().job_id)[:8]}] --> "
          f"[{str(child.first().job_id)[:8]}]")

def ls(args):
    objects = args.objects
    name = args.name
    history = args.history
    verbose = args.verbose
    id = args.id
    tree = args.tree
    wf = args.wf

    if objects.startswith('job'):
        lscmd.ls_jobs(name, history, id, verbose, tree, wf)
    elif objects.startswith('app'):
        lscmd.ls_apps(name, id, verbose)
    elif objects.startswith('work') or objects.startswith('wf'):
        lscmd.ls_wf(name, verbose, tree, wf)

def modify(args):
    pass

def rm(args):
    pass

def qsub(args):
    job = Job()
    job.name = args.name
    job.description = 'Added by balsam qsub'
    job.workflow = 'qsub'
    job.allowed_work_sites = settings.BALSAM_SITE

    job.wall_time_minutes = args.wall_minutes
    job.num_nodes = args.nodes
    job.processes_per_node = args.ppn
    job.threads_per_rank = args.threads_per_rank
    job.threads_per_core = args.threads_per_core
    job.environ_vars = ":".join(args.env)

    job.application = ''
    job.application_args = ''
    job.preprocess = ''
    job.postprocess = ''
    job.post_error_handler = False
    job.post_timeout_handler = False
    job.auto_timeout_retry = False
    job.input_files = ''
    job.stage_in_url = ''
    job.stage_out_url = ''
    job.stage_out_files = ''
    job.direct_command = ' '.join(args.command)

    print(job)
    job.save()
    print("Added to database")

def kill(args):
    job_id = args.id
    
    job = Job.objects.filter(job_id__startswith=job_id)
    if job.count() > 1:
        raise RuntimeError(f"More than one job matches {job_id}")
    if job.count() == 0:
        print(f"No jobs match the given ID {job_id}")

    job = job.first()

    if cmd_confirmation(f'Really kill job {job.name} [{str(job.pk)}] ??'):
        dag.kill(job, recursive=args.recursive)
        print("Job killed")


def mkchild(args):
    if not dag.current_job:
        raise RuntimeError(f"mkchild requires that BALSAM_JOB_ID is in the environment")
    child_job = newjob(args)
    dag.add_dependency(dag.current_job, child_job)
    print(f"Created link [{str(dag.current_job.job_id)[:8]}] --> "
          f"[{str(child_job.job_id)[:8]}]")

def launcher(args):
    pass

def service(args):
    pass
