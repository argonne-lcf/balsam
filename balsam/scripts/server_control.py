from balsam.django_config import serverinfo
from filelock import FileLock
import sys
import time
import socket
import subprocess
import os

def launch_server(info):
    info.reset_server_address()
    start_cmd = f"pg_ctl -w start -D {info['pg_db_path']}"
    print("Launching Balsam DB server")
    proc = subprocess.run(start_cmd, shell=True, check=True)
    time.sleep(1)

def kill_server(info):
    local_host = socket.gethostname()
    server_host = info['host']
    stop_cmd = f"pg_ctl -w stop -D {info['pg_db_path']}"

    if server_host == local_host:
        print("Stopping local Balsam DB server")
        subprocess.run(stop_cmd, shell=True)
        time.sleep(1)
    else:
        print("Stopping remote Balsam DB server")
        subprocess.run(f"ssh {server_host} {stop_cmd}", shell=True)

    info.update(dict(host='', port='', active_clients=[]))

def disconnect_main(db_path):
    lockfile = os.path.join(db_path, serverinfo.ADDRESS_FNAME+'.lock')

    with FileLock(lockfile, timeout=5):
        info = serverinfo.ServerInfo(db_path)

        client_id = [socket.gethostname(), os.getppid()]
        active_clients = info.get('active_clients', [])
        # get PS list on the current host
        # remove any entries from "active_clients" if they match the current host and process is no longer running
        active_clients = [cid for cid in active_clients if cid != client_id]
        info.update({'active_clients' : active_clients})
        if len(active_clients) == 0:
            kill_server(info)

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


def start_main(db_path):

    lockfile = os.path.join(db_path, serverinfo.ADDRESS_FNAME+'.lock')
    with FileLock(lockfile, timeout=5):
        info = serverinfo.ServerInfo(db_path)

        if not (info['host'] or info['port']):
            launch_server(info)
            test_connection(info, raises=True)
            is_active = True
        else:
            is_active = test_connection(info)

        if not is_active:
            print(f"Server at {info['host']}:{info['port']} isn't responsive; will try to kill and restart")
            kill_server(info)
            launch_server(info)
            test_connection(info, raises=True)
        else:
            print("Connected to existing Balsam DB server")

        client_id = [socket.gethostname(), os.getppid()]
        active_clients = info.get('active_clients', [])
        if client_id not in active_clients:
            active_clients.append(client_id)
            info.update({'active_clients' : active_clients})


if __name__ == "__main__":
    db_path = os.environ.get('BALSAM_DB_PATH', None)
    if db_path:
        start_main(db_path)
    else:
        raise RuntimeError('BALSAM_DB_PATH needs to be set before server can be started\n')
