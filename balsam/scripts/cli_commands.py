import getpass
import os
from importlib.util import find_spec
import subprocess
import signal
import sys

import django

def ls_procs(keywords):
    if type(keywords) == str: keywords = [keywords]

    username = getpass.getuser()
    
    searchcmd = 'ps aux | grep '
    searchcmd += ' | grep '.join(f'"{k}"' for k in keywords) 
    grep = subprocess.Popen(searchcmd, shell=True, stdout=subprocess.PIPE)
    stdout,stderr = grep.communicate()
    stdout = stdout.decode('utf-8')

    processes = [line for line in stdout.split('\n') if 'python' in line and line.split()[0]==username]
    return processes

def cmd_confirmation(message=''):
    confirm = ''
    while not confirm.lower() in ['y', 'n']:
        try:
            confirm = input(f"{message} [y/n]: ")
        except: pass
    return confirm.lower() == 'y'

def newapp(args):
    from balsam import setup
    setup()
    from balsam.core.models import ApplicationDefinition as AppDef

    def py_app_path(path):
        if not path: 
            return ''
        args = path.split()
        app = args[0]
        if app.endswith('.py'):
            exe = sys.executable
            script_path = os.path.abspath(app)
            args = ' '.join(args[1:])
            path = ' '.join((exe, script_path, args))
        return path

    if AppDef.objects.filter(name=args.name).exists():
        raise RuntimeError(f"An application named {args.name} already exists")
    
    app = AppDef()
    app.name = args.name
    app.description = ' '.join(args.description) if args.description else ''
    app.executable = py_app_path(args.executable)
    app.preprocess = py_app_path(args.preprocess)
    app.postprocess = py_app_path(args.postprocess)
    app.save()
    print(app)
    print("Added app to database")


def newjob(args):
    from balsam import setup
    setup()
    from balsam.core import models
    Job = models.BalsamJob
    AppDef = models.ApplicationDefinition

    if not AppDef.objects.filter(name=args.application).exists():
        raise RuntimeError(f"App {args.application} not registered in local DB")

    job = Job()
    job.name = args.name
    job.description = ' '.join(args.description)
    job.workflow = args.workflow

    job.wall_time_minutes = args.wall_time_minutes
    job.num_nodes = args.num_nodes
    job.coschedule_num_nodes = args.coschedule_num_nodes
    job.node_packing_count = args.node_packing_count
    job.ranks_per_node = args.ranks_per_node
    job.threads_per_rank = args.threads_per_rank
    job.threads_per_core = args.threads_per_core

    job.application = args.application
    job.args = ' '.join(args.args)
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
    from balsam import setup
    setup()
    from balsam.core import models
    Job = models.BalsamJob

    job = Job.objects.filter(job_id__icontains=s)
    count = job.count()
    if count > 1:
        raise ValueError(f"More than one ID matched {s}")
    elif count == 1:
        return job.first()
    else:
        raise ValueError(f"No job in local DB matched {s}")

def newdep(args):
    from balsam import setup
    setup()
    from balsam.core import models
    from balsam.launcher import dag
    Job = models.BalsamJob
    AppDef = models.ApplicationDefinition

    parent = match_uniq_job(args.parent)
    child = match_uniq_job(args.child)
    dag.add_dependency(parent, child)
    print(f"Created link {parent.cute_id} --> {child.cute_id}")

def ls(args):
    from balsam import setup
    setup()
    from balsam.core import models
    from balsam.launcher import dag
    import balsam.scripts.ls_commands as lscmd
    Job = models.BalsamJob
    AppDef = models.ApplicationDefinition

    objects = args.objects
    name = args.name
    history = args.history
    verbose = args.verbose
    state = args.state
    id = args.id
    tree = args.tree
    wf = args.wf
    by_states = args.by_states

    try:
        if objects.startswith('job'):
            lscmd.ls_jobs(name, history, id, verbose, tree, wf, state, by_states)
        elif objects.startswith('app'):
            lscmd.ls_apps(name, id, verbose)
        elif objects.startswith('work') or objects.startswith('wf'):
            lscmd.ls_wf(name, verbose, tree, wf)
        elif objects.startswith('queues'):
            lscmd.ls_queues(verbose)
    except (KeyboardInterrupt,BrokenPipeError):
        pass

def modify(args):
    from balsam import setup
    setup()
    from balsam.core import models
    Job = models.BalsamJob
    AppDef = models.ApplicationDefinition

    if args.type == 'jobs': cls = Job
    elif args.type == 'apps': cls = AppDef

    item = cls.objects.filter(pk__contains=args.id)
    if item.count() == 0:
        raise RuntimeError(f"no matching {args.type}")
    elif item.count() > 1:
        raise RuntimeError(f"more than one matching {args.type}")
    item = item.first()

    target_type = type(getattr(item, args.attr))
    new_value = target_type(args.value)
    if args.attr == 'state':
        if item.state == 'USER_KILLED':
            print("Cannot mutate state of a killed job")
            return
        item.update_state(new_value, 'User mutated state from command line')
    else:
        setattr(item, args.attr, new_value)
        item.save()
    print(f'{args.type[:-1]} {args.attr} changed to:  {new_value}')


def rm(args):
    from balsam import setup
    setup()
    from balsam.core import models
    from balsam.launcher import dag
    Job = models.BalsamJob
    AppDef = models.ApplicationDefinition

    objects_name = args.objects
    name = args.name
    objid = args.id
    deleteall = args.all
    force = args.force

    # Are we removing jobs or apps?
    if objects_name.startswith('job'): cls = Job
    elif objects_name.startswith('app'): cls = AppDef
    objects = cls.objects

    # Filter: all objects, by name-match (multiple), or by ID (unique)?
    if deleteall:
        deletion_objs = objects.all()
        message = f"ALL {objects_name}"
    elif name: 
        deletion_objs = objects.filter(name__icontains=name)
        message = f"{len(deletion_objs)} {objects_name} matching name {name}"
        if not deletion_objs.exists(): 
            print("No {objects_name} matching query")
            return
    elif objid: 
        deletion_objs = objects.filter(pk__icontains=objid)
        if deletion_objs.count() > 1:
            raise RuntimeError(f"Multiple {objects_name} match ID")
        elif deletion_objs.count() == 0:
            raise RuntimeError(f"No {objects_name} match ID")
        else:
            message = f"{objects_name[:-1]} with ID matching {objid}"
    
    # User confirmation
    if not force:
        if not cmd_confirmation(f"PERMANENTLY remove {message}?"):
            print("Delete aborted")
            return

    # Actually delete things here
    deletion_objs.delete()
    print("Deleted.")

def kill(args):
    from balsam import setup
    setup()
    from balsam.core import models
    from balsam.launcher import dag
    Job = models.BalsamJob

    job_id = args.id
    
    job = Job.objects.filter(job_id__startswith=job_id)
    if job.count() > 1:
        raise RuntimeError(f"More than one job matches {job_id}")
    if job.count() == 0:
        print(f"No jobs match the given ID {job_id}")

    job = job.first()

    if cmd_confirmation(f'Really kill job {job.name} {job.cute_id} ??'):
        dag.kill(job, recursive=args.recursive)
        print("Job killed")


def mkchild(args):
    from balsam import setup
    setup()
    from balsam.launcher import dag

    if not dag.current_job:
        raise RuntimeError(f"mkchild requires that BALSAM_JOB_ID is in the environment")
    child_job = newjob(args)
    dag.add_dependency(dag.current_job, child_job)
    print(f"Created link {dag.current_job.cute_id} --> {child_job.cute_id}")

def launcher(args):
    fname = find_spec("balsam.launcher.launcher").origin
    original_args = sys.argv[2:]
    command = [sys.executable] + [fname] + original_args
    p = subprocess.Popen(command)
    print(f"Started Balsam launcher [{p.pid}]")
    p.wait()

def submitlaunch(args):
    from balsam import setup
    setup()
    from balsam.service import service
    from balsam.core import models
    from django.db import connection, transaction

    # Exclusive Lock on core_queuedlaunch
    with transaction.atomic():
        with connection.cursor() as cursor:
            cursor.execute('LOCK TABLE core_queuedlaunch IN ACCESS EXCLUSIVE MODE;')
            QueuedLaunch = models.QueuedLaunch
            qlaunch = QueuedLaunch(
                    project = args.project,
                    queue = args.queue,
                    nodes = args.nodes,
                    wall_minutes = args.time_minutes,
                    job_mode = args.job_mode,
                    wf_filter = args.wf_filter,
                    prescheduled_only=False)
            qlaunch.save()
            service.submit_qlaunch(qlaunch, verbose=True)

def service(args):
    fname = find_spec("balsam.service.service").origin
    original_args = sys.argv[2:]
    command = [sys.executable] + [fname] + original_args
    p = subprocess.Popen(command)
    print(f"Starting Balsam service [{p.pid}]")

def init(args):
    path = os.path.expanduser(args.path)
    if os.path.exists(path):
        if not os.path.isdir(path):
            print(f"{path} is not a directory")
            sys.exit(1)
    else:
        try:
            os.mkdir(path, mode=0o755)
        except:
            print(f"Failed to create directory {path}")
            sys.exit(1)
        
    fname = find_spec("balsam.scripts.init").origin
    p = subprocess.Popen(f'{sys.executable} {fname} {path}', shell=True)
    p.wait()

def which(args):
    from balsam.django_config.db_index import refresh_db_index
    from balsam.django_config.serverinfo import ServerInfo
    import pprint

    if args.list:
        pprint.pprint(refresh_db_index())
    elif args.name:
        db_list = refresh_db_index()

        # Try exact match first
        name = os.path.abspath(os.path.expanduser(args.name))
        if name in db_list:
            print(name)
            sys.exit(0)

        matches = [db for db in db_list if args.name in db]
        if len(matches) == 0:
            sys.stderr.write(f"No DB matching {args.name} is cached\n\n")
            sys.exit(1)
        elif len(matches) > 1:
            sys.stderr.write(f"DB name {args.name} is too ambiguous; multiple matches\n"
                              "Please give a longer unique substring\n\n"
                            )
            sys.exit(1)
        else:
            print(matches[0])
            sys.exit(0)
    else:
        db_path = os.environ.get('BALSAM_DB_PATH')
        if db_path:
            print("Current Balsam DB:", db_path)
            dat = ServerInfo(os.environ['BALSAM_DB_PATH']).data
            pprint.pprint(dat)
        else:
            print("BALSAM_DB_PATH is not set")
            print('Use "source balsamactivate" to activate one of these existing databases:')
            pprint.pprint(refresh_db_index())

def log(args):
    from balsam import settings, setup
    setup()
    path = os.path.join(settings.LOGGING_DIRECTORY, '*.log')
    try: subprocess.run(f"tail -f {path}", shell=True)
    except (KeyboardInterrupt,BrokenPipeError,ProcessLookupError): pass


def server(args):
    from balsam.scripts import postgres_control
    db_path = os.environ.get('BALSAM_DB_PATH', None)
    if not db_path:
        raise RuntimeError('BALSAM_DB_PATH needs to be set before server can be started\n')

    if args.connect:
        postgres_control.start_main(db_path)
    elif args.reset:
        postgres_control.reset_main(db_path)
    elif args.list_active_connections:
        postgres_control.list_connections(db_path)
    elif args.add_user:
        uname = args.add_user
        if len(uname) == 0: raise RuntimeError("Please provide user name")
        postgres_control.add_user(db_path, uname)
    elif args.drop_user:
        uname = args.drop_user
        if len(uname) == 0: raise RuntimeError("Please provide user name")
        postgres_control.drop_user(db_path, uname)
    elif args.list_users:
        postgres_control.list_users(db_path)

def make_dummies(args):
    from balsam import setup
    setup()
    from balsam.core import models
    Job = models.BalsamJob
    App = models.ApplicationDefinition
    if not App.objects.filter(name='dummy').exists():
        dummy_app = App(name="dummy", executable="echo")
        dummy_app.save()

    jobs = [Job(
                name = f'dummy{i}',
                description = 'Added by balsam make_dummies',
                node_packing_count = 64,
                workflow = 'dummy',
                application = 'dummy',
                args = 'hello'
               )
            for i in range(args.num)]
    Job.objects.bulk_create(jobs)
    print(f"Added {args.num} dummy jobs to the DB")
