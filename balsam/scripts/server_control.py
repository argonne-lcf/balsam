from balsam.django_config import serverinfo
import sys
import time
import socket
import subprocess
import os

def ps_list():
    def tryint(s):
        try: return int(s)
        except ValueError: pass
    ps = subprocess.run("ps aux | awk '{print $2}'", shell=True, stdout=subprocess.PIPE)
    pids = ps.stdout.decode('utf-8').split('\n')
    pids = [tryint(pid) for pid in pids]
    pids = [pid for pid in pids if type(pid) is int]
    return pids

def launch_server(info):
    info.reset_server_address()
    log_path = os.path.join(os.path.expanduser('~'), '.balsam', 'postgres.log')
    start_cmd = f"pg_ctl -w start -D {info['pg_db_path']} -l {log_path} --mode=smart"
    print("Launching Balsam DB server")
    proc = subprocess.run(start_cmd, shell=True, check=True)

def kill_server(info):
    local_host = socket.gethostname()
    server_host = info['host']
    stop_cmd = f"pg_ctl -w stop -D {info['pg_db_path']} --mode=smart"

    if server_host == local_host:
        print("Stopping local Balsam DB server")
        subprocess.run(stop_cmd, shell=True)
        time.sleep(1)
    else:
        print("Stopping remote Balsam DB server")
        subprocess.run(f"ssh {server_host} {stop_cmd}", shell=True)
    info.update(dict(host='', port='', active_clients=[]))


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
    
    from balsam.launcher.dag import BalsamJob
    BalsamJob.release_all_locks()

    lockfile = os.path.join(db_path, serverinfo.ADDRESS_FNAME+'.lock')
    info = serverinfo.ServerInfo(db_path)
    kill_server(info)


def disconnect_main(db_path):
    lockfile = os.path.join(db_path, serverinfo.ADDRESS_FNAME+'.lock')
    info = serverinfo.ServerInfo(db_path)

    local_host = socket.gethostname()
    local_ps_list = ps_list()
    client_id = [local_host, os.getppid()]

    active_clients = info.get('active_clients', [])
    disconnected_clients = [cid for cid in active_clients
                            if (cid[0] == local_host) and (cid[1] not in local_ps_list)]
    disconnected_clients.append(client_id)

    active_clients = [cid for cid in active_clients if cid not in disconnected_clients]
    info.update({'active_clients' : active_clients})
    if len(active_clients) == 0:
        kill_server(info)


def start_main(db_path):
    lockfile = os.path.join(db_path, serverinfo.ADDRESS_FNAME+'.lock')
    info = serverinfo.ServerInfo(db_path)

    if not (info['host'] or info['port']):
        launch_server(info)
        test_connection(info, raises=True)
    elif test_connection(info):
        print("Connected to already running Balsam DB server!")
    else:
        print(f"Server at {info['host']}:{info['port']} isn't responsive; will try to kill and restart")
        kill_server(info)
        launch_server(info)
        test_connection(info, raises=True)

    client_id = [socket.gethostname(), os.getppid()]
    active_clients = info.get('active_clients', [])
    if client_id not in active_clients:
        active_clients.append(client_id)
        info.update({'active_clients' : active_clients})

def list_connections(db_path):
    info = serverinfo.ServerInfo(db_path)
    host = info['host']
    port = info['port']
    subprocess.run(
        f'''psql -d balsam -h {host} -p {port} -c \
        "SELECT pid,application_name,usename,state,substr(query, 1, 60)\
        FROM pg_stat_activity WHERE datname = 'balsam';"
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
        GRANT ALL on service_balsamjob TO {uname}; \
        GRANT ALL on service_applicationdefinition TO {uname}; \
        GRANT ALL on service_queuedlaunch TO {uname}; "
        ''',
        shell=True
    )

def drop_user(db_path, uname):
    info = serverinfo.ServerInfo(db_path)
    host = info['host']
    port = info['port']
    subprocess.run(
        f'psql -d balsam -h {host} -p {port} -c "REVOKE ALL on service_balsamjob FROM {uname};\
        REVOKE ALL on service_applicationdefinition FROM {uname};\
        REVOKE ALL on service_queuedlaunch FROM {uname};\
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
