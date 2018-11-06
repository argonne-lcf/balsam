import glob
import os
import sys
from pprint import pprint
import subprocess
from balsam import setup, settings
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from balsam.django_config.serverinfo import ServerInfo
from balsam.scripts import postgres_control
from django.db import connection

def postgres_init(serverInfo):
    if not os.path.exists(serverInfo.pg_db_path): # postgres data directory
        postgres_control.create_db(serverInfo)
    else:
        print("Postgres data directory already exists; will not create new DB")
        postgres_control.start_main(serverInfo.balsam_db_path)
        serverInfo.refresh()

def run_migrations():
    from django import db
    from django.core.management import call_command
    from balsam.django_config.db_index import refresh_db_index
    setup()
    print(f"DB settings:", settings.DATABASES['default'])
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
    connection.close()
    postgres_control.kill_server(serverInfo)

    base = dbpath[:-1] if dbpath[-1]=='/' else dbpath
    basename = os.path.basename(base)
    msg = f"  Successfully created Balsam DB at: {os.environ['BALSAM_DB_PATH']}  "
    print('\n' + '*'*len(msg))
    print(msg)
    print(f"  Use `source balsamactivate {basename}` to begin working.")
    print('*'*len(msg)+'\n')
