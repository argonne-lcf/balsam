from balsam.django_config import serverinfo
from .infolock import InfoLock
import sys
import time
import socket
import signal
import subprocess
import os

class SignalReceived(Exception): pass
def term_handler(signum, stack):
    raise SignalReceived(f"Killed by signal {signum}")

def launch_server(info):
    if not info._is_owner:
        raise PermissionError("Please ask the owner of this DB to launch it")
    info.reset_server_address()
    log_path = os.path.join(info.balsam_db_path, 'postgres.log')
    user_home = os.path.join(os.path.expanduser('~'), '.balsam')
    if not os.path.exists(user_home):
        print("Creating ~/.balsam directory")
        os.makedirs(user_home)
    start_cmd = f"pg_ctl -w start -D {info.pg_db_path} -l {log_path} --mode=smart"
    print("Launching Balsam DB server")
    proc = subprocess.run(start_cmd, shell=True, check=True)

def kill_server(info):
    if not info._is_owner:
        raise PermissionError("Only the owner of the server-info can kill this Balsam DB")
    local_host = socket.gethostname()
    server_host = info['host']
    stop_cmd = f"pg_ctl -w stop -D {info.pg_db_path} --mode=smart"

    if server_host == local_host:
        print("Stopping local Balsam DB server")
        subprocess.run(stop_cmd, shell=True)
        time.sleep(1)
    else:
        print("Stopping remote Balsam DB server")
        subprocess.run(f"ssh {server_host} {stop_cmd}", shell=True)
    info.update(dict(host='', port=''))


def test_connection(info, raises=False):
    from balsam.launcher import dag
    from django.conf import settings
    from django import db

    # reset connection in case serverinfo changed *after* Django setup
    db.connections.close_all()
    settings.DATABASES['default']['PORT'] = info['port']
    settings.DATABASES['default']['HOST'] = info['host']

    try:
        count = dag.BalsamJob.objects.count()
    except db.OperationalError:
        if raises: raise
        else: return False
    else:
        assert type(count) is int
        return True

def reset_main(db_path):
    start_main(db_path)
    info = serverinfo.ServerInfo(db_path)
    kill_server(info)

def start_main(db_path):
    old_handler = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, term_handler)
    with InfoLock(db_path):
        _start_main(db_path)
    signal.signal(signal.SIGTERM, old_handler)

def _start_main(db_path):
    info = serverinfo.ServerInfo(db_path)

    if not (info.get('host') or info.get('port')):
        launch_server(info)
        test_connection(info, raises=True)
    elif test_connection(info):
        print("Connected to already running Balsam DB server!")
    elif info._is_owner:
        print(f"Server at {info['host']}:{info['port']} isn't responsive; will try to kill and restart")
        kill_server(info)
        launch_server(info)
        test_connection(info, raises=True)
    else:
        print(f"Server at {info['host']}:{info['port']} isn't responsive; please ask the owner to restart it")

def list_connections(db_path):
    info = serverinfo.ServerInfo(db_path)
    host = info['host']
    port = info['port']
    subprocess.run(
        f'''psql -d balsam -h {host} -p {port} -c \
        "SELECT pid,application_name,usename,state,substr(query, 1, 60)\
        FROM pg_stat_activity WHERE datname = 'balsam';" \
        ''',
        shell=True
    )

def add_user(db_path, uname):
    info = serverinfo.ServerInfo(db_path)
    host = info['host']
    port = info['port']
    subprocess.run(
        f'''psql -d balsam -h {host} -p {port} -c \
        "CREATE user {uname}; \
        grant all privileges on database balsam to {uname}; \
        GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {uname}; \
        GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO {uname}; "\
        ''',
        shell=True
    )

def drop_user(db_path, uname):
    info = serverinfo.ServerInfo(db_path)
    host = info['host']
    port = info['port']
    subprocess.run(
        f'psql -d balsam -h {host} -p {port} -c \
        "REVOKE ALL privileges on all tables in schema public FROM {uname};\
        REVOKE ALL privileges on all sequences in schema public FROM {uname};\
        REVOKE ALL privileges on database balsam FROM {uname};\
        DROP ROLE {uname};"',
        shell=True
    )

def list_users(db_path):
    info = serverinfo.ServerInfo(db_path)
    host = info['host']
    port = info['port']
    subprocess.run(
        f'''psql -d balsam -h {host} -p {port} -c \
        "SELECT rolname FROM pg_roles;"
        ''',
        shell=True
    )

def create_db(serverInfo):
    db_path = serverInfo.pg_db_path
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
    launch_server(serverInfo)
    serverInfo.refresh()
    port = serverInfo['port']
    create_proc = subprocess.Popen(f'createdb balsam -p {port}', shell=True)
    retcode = create_proc.wait()
    if retcode != 0: raise RuntimeError("createdb failed")
    else: print("Created `balsam` DB")
