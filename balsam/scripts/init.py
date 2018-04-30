from getpass import getuser
import os
import sys
from pprint import pprint
import time
import subprocess
try:
    from balsam.django_config.serverinfo import ServerInfo
except:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    from balsam.django_config.serverinfo import ServerInfo

def sqlite3_init(serverInfo):
    pass

def postgres_init(serverInfo):
    db_path = serverInfo['balsamdb_path']
    db_path = os.path.join(db_path, 'balsamdb')
    p = subprocess.Popen(f'initdb -D {db_path} -U $USER', shell=True)
    retcode = p.wait()
    if retcode != 0: raise RuntimeError("initdb failed")
    
    with open(os.path.join(db_path, 'postgresql.conf'), 'a') as fp:
        fp.write("listen_addresses = '*' # appended from balsam init\n")
        fp.write('port=0 # appended from balsam init\n')
        fp.write('max_connections=128 # appended from balsam init\n')
        fp.write('shared_buffers=2GB # appended from balsam init\n')
        fp.write('synchronous_commit=off # appended from balsam init\n')
        fp.write('wal_writer_delay=400ms # appended from balsam init\n')
    
    with open(os.path.join(db_path, 'pg_hba.conf'), 'a') as fp:
        fp.write(f"host all all 0.0.0.0/0 trust\n")

    serverInfo.update({'user' : getuser()})
    serverInfo.reset_server_address()
    port = serverInfo['port']

    serv_proc = subprocess.Popen(f'pg_ctl -D {db_path} -w start', shell=True)
    time.sleep(2)
    create_proc = subprocess.Popen(f'createdb balsam -p {port}', shell=True)
    retcode = create_proc.wait()
    if retcode != 0: raise RuntimeError("createdb failed")

def postgres_post(serverInfo):
    db_path = serverInfo['balsamdb_path']
    db_path = os.path.join(db_path, 'balsamdb')
    serv_proc = subprocess.Popen(f'pg_ctl -D {db_path} -w stop', shell=True)
    time.sleep(1)
    serverInfo.update({'host':None, 'port':None})

def run_migrations():
    import django
    os.environ['DJANGO_SETTINGS_MODULE'] = 'balsam.django_config.settings'
    django.setup()
    
    from django import db
    from django.core.management import call_command
    from django.conf import settings
    from balsam.django_config.db_index import refresh_db_index

    print(f"DB settings:", settings.DATABASES['default'])

    db_info = db.connection.settings_dict['NAME']
    print(f"Setting up new balsam database:")
    pprint(db_info, width=60)
    call_command('makemigrations', interactive=False, verbosity=0)
    call_command('migrate', interactive=False, verbosity=0)
    refresh_db_index()

    try:
        from balsam.service.models import BalsamJob
        j = BalsamJob()
        j.save()
        j.delete()
    except:
        raise RuntimeError("BalsamJob table not properly created")
    else:
        print("BalsamJob table created successfully")

if __name__ == "__main__":
    serverInfo = ServerInfo(sys.argv[1])
    db_type = serverInfo['db_type']

    if db_type == 'sqlite3':
        sqlite3_init(serverInfo)
    elif db_type == 'postgres':
        postgres_init(serverInfo)
    else:
        raise RuntimeError(f'init doesnt support DB type {db_type}')

    run_migrations()
    if db_type == 'postgres':
        postgres_post(serverInfo)
        print("OK")
