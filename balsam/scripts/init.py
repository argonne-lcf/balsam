from getpass import getuser
import os
import sys
import time
import subprocess
from balsam.django_config.serverinfo import ServerInfo

def sqlite3_init(serverInfo):
    pass

def postgres_init(serverInfo):
    db_path = serverInfo['balsamdb_path']
    db_path = os.path.join(db_path, 'balsamdb')
    p = subprocess.Popen(f'initdb -D {db_path} -U $USER', shell=True)
    retcode = p.wait()
    if retcode != 0: raise RuntimeError("initdb failed")

    serverInfo.update({'user' : getuser()})
    serverInfo.reset_server_address()
    port = serverInfo['port']
    with open(os.path.join(db_path, 'postgresql.conf'), 'a') as fp:
        fp.write(f'port={port} # appended from balsam init\n')

    serv_proc = subprocess.Popen(f'pg_ctl -D {db_path} -w start', shell=True)
    time.sleep(2)
    create_proc = subprocess.Popen(f'createdb balsam -p {port}', shell=True)
    retcode = create_proc.wait()
    if retcode != 0: raise RuntimeError("createdb failed")

def postgres_post(serverInfo):
    db_path = serverInfo['balsamdb_path']
    db_path = os.path.join(db_path, 'balsamdb')
    serv_proc = subprocess.Popen(f'pg_ctl -D {db_path} -w stop', shell=True)
    serv_proc.wait()


def run_migrations():
    import django
    os.environ['DJANGO_SETTINGS_MODULE'] = 'balsam.django_config.settings'
    django.setup()
    
    from django import db
    from django.core.management import call_command
    from django.conf import settings

    print(f"DB settings:", settings.DATABASES['default'])

    db_path = db.connection.settings_dict['NAME']
    print(f"Setting up new balsam database: {db_path}")
    call_command('makemigrations', interactive=False, verbosity=0)
    call_command('migrate', interactive=False, verbosity=0)

    new_path = settings.DATABASES['default']['NAME']
    if os.path.exists(new_path):
        print(f"Set up new DB at {new_path}")
    else:
        raise RuntimeError(f"Failed to created DB at {new_path}")

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
