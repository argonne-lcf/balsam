import argparse
from importlib.util import find_spec
import glob
import getpass
import os
import sys
import signal
import subprocess
import time

os.environ['IS_SERVER_DAEMON']="True"

from balsam.django_config.settings import resolve_db_path
from serverinfo import ServerInfo

CHECK_PERIOD = 4
TERM_LINGER = 30
PYTHON = sys.executable
SQLITE_SERVER = find_spec('balsam.django_config.sqlite_server').origin
DB_COMMANDS = {
    'sqlite3' : f'{PYTHON} {SQLITE_SERVER}',
    'postgres': f'pg_ctl -D {{pg_db_path}} -w start',
    'mysql'   : f'',
}
term_start = 0


def run(cmd):
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.DEVNULL, 
                            stderr=subprocess.STDOUT)
    return proc

def stop(proc, serverinfo):
    print("Balsam server shutdown...", flush=True)
    if serverinfo['db_type'] == 'postgres':
        cmd = f'pg_ctl -D {{pg_db_path}} -w stop'.format(**serverinfo.data)
        print(cmd)
        proc = subprocess.Popen(cmd, shell=True)
        time.sleep(2)
    else:
        proc.terminate()
        try: retcode = proc.wait(timeout=30)
        except subprocess.TimeoutExpired: 
            print("Warning: server did not quit gracefully")
            proc.kill()

def wait(proc, serverinfo):
    if serverinfo['db_type'] == 'sqlite3':
        retcode = proc.wait(timeout=CHECK_PERIOD)
    elif serverinfo['db_type'] == 'postgres':
        time.sleep(CHECK_PERIOD)

        user = getpass.getuser()
        proc = subprocess.Popen('ps aux | grep {user} | grep postgres | '
        'grep -v grep', shell=True, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)
        stdout, _ = proc.communicate()

        lines = stdout.decode('utf-8').split('\n')
        if len(lines) >= 1: raise subprocess.TimeoutExpired('cmd', CHECK_PERIOD)
        
def main(db_path):

    serverinfo = ServerInfo(db_path)
    serverinfo.reset_server_address()
    server_type = serverinfo['db_type']

    db_cmd = f"BALSAM_DB_PATH={db_path} " + DB_COMMANDS[server_type].format(**serverinfo.data)
    print(f"\nStarting balsam DB server daemon for DB at {db_path}")
    print(db_cmd)

    proc = run(db_cmd)

    # On SIGUSR1, stop immediately ("balsam server --stop" does this)
    def handle_stop(signum, stack):
        stop(proc, serverinfo)
        serverinfo.update({'address': None, 'host':None,'port':None})
        sys.exit(0)
    
    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGUSR1, handle_stop)

    while not term_start or time.time() - term_start < TERM_LINGER:
        try: 
            wait(proc, serverinfo)
        except subprocess.TimeoutExpired: 
            pass
        else:
            print("\nserver process stopped unexpectedly; restarting")
            serverinfo.reset_server_address()
            db_cmd = f"BALSAM_DB_PATH={db_path} " + DB_COMMANDS[server_type].format(**serverinfo.data)
            print(db_cmd)
            proc = run(db_cmd)
    
    stop(proc, serverinfo)
    serverinfo.update({'address': None, 'host':None,'port':None})

if __name__ == "__main__":
    input_path = sys.argv[1] if len(sys.argv) == 2 else None
    db_path = resolve_db_path(input_path)
    main(db_path)
