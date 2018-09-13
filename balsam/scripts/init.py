import glob
import os
import sys
from pprint import pprint
import subprocess
from balsam import setup, settings
try:
    from balsam.django_config.serverinfo import ServerInfo
except:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    from balsam.django_config.serverinfo import ServerInfo

def postgres_init(serverInfo):
    db_path = serverInfo['balsamdb_path']
    db_path = os.path.join(db_path, 'balsamdb')
    if os.path.exists(db_path): 
        print(db_path, "already exists; skipping DB creation")
        return

    retcode = subprocess.Popen(f'initdb -D {db_path} -U $USER', shell=True).wait()
    if retcode != 0: raise RuntimeError("initdb process failed")
    
    with open(os.path.join(db_path, 'postgresql.conf'), 'a') as fp:
        fp.write("listen_addresses = '*' # appended from balsam init\n")
        fp.write('port=0 # appended from balsam init\n')
        fp.write('max_connections=128 # appended from balsam init\n')
        fp.write('shared_buffers=2GB # appended from balsam init\n')
        fp.write('synchronous_commit=off # appended from balsam init\n')
        fp.write('wal_writer_delay=400ms # appended from balsam init\n')
        fp.write('logging_collector=on # appended from balsam init\n')
        fp.write('log_min_duration_statement=0 # appended from balsam init\n')
        fp.write('log_connections=on # appended from balsam init\n')
        fp.write('log_duration=on # appended from balsam init\n')
    
    with open(os.path.join(db_path, 'pg_hba.conf'), 'a') as fp:
        fp.write(f"host all all 0.0.0.0/0 trust\n")

    serverInfo.reset_server_address()
    port = serverInfo['port']

    retcode = subprocess.Popen(f'pg_ctl -D {db_path} -w start', shell=True).wait()
    if retcode == 0: print("started PG server")
    else: raise RuntimeError("pg_ctl server startup process failed")

    create_proc = subprocess.Popen(f'createdb balsam -p {port}', shell=True)
    retcode = create_proc.wait()
    if retcode != 0: raise RuntimeError("createdb failed")
    else: print("Created `balsam` DB")

def postgres_post(serverInfo):
    db_path = serverInfo['balsamdb_path']
    db_path = os.path.join(db_path, 'balsamdb')
    serv_proc = subprocess.Popen(f'pg_ctl -D {db_path} -w stop', shell=True).wait()
    serverInfo.update({'host':None, 'port':None})

def run_migrations():
    from django import db
    from django.core.management import call_command
    from balsam.django_config.db_index import refresh_db_index
    from balsam.core import migrations
    setup()
    path = migrations.__path__
    try: path = path[0]
    except: path = str(path)
    for fname in glob.glob(os.path.join(path, '????_*.py')):
        os.remove(fname)
        print("Remove migration file:", fname)

    print(f"DB settings:", settings.DATABASES['default'])
    print("Setting up BalsamJob table")
    call_command('makemigrations', interactive=True, verbosity=2)
    call_command('migrate', interactive=True, verbosity=2)
    refresh_db_index()
    try:
        from balsam.core.models import BalsamJob
        j = BalsamJob()
        j.save()
        j.delete()
    except:
        print("BalsamJob table not properly created")
        raise
    else:
        print("BalsamJob table created successfully")

if __name__ == "__main__":
    dbpath = sys.argv[1]
    os.environ['BALSAM_DB_PATH'] = dbpath
    serverInfo = ServerInfo(dbpath)

    postgres_init(serverInfo)
    run_migrations()
    postgres_post(serverInfo)

    base = dbpath[:-1] if dbpath[-1]=='/' else dbpath
    basename = os.path.basename(base)
    msg = f"  Successfully created Balsam DB at: {os.environ['BALSAM_DB_PATH']}  "
    print('\n' + '*'*len(msg))
    print(msg)
    print(f"  Use `source balsamactivate {basename}` to begin working.")
    print('*'*len(msg)+'\n')
