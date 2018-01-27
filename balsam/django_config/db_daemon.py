import argparse
from importlib.util import find_spec
import glob
import os
import sys
import signal
import subprocess

os.environ['IS_SERVER_DAEMON']="True"

from balsam.django_config.settings import resolve_db_path
from serverinfo import ServerInfo

CHECK_PERIOD = 4
TERM_LINGER = 30
PYTHON = sys.executable
SQLITE_SERVER = find_spec('balsam.django_config.sqlite_server').origin
DB_COMMANDS = {
    'sqlite3' : f'{PYTHON} {SQLITE_SERVER}',
    'postgres': f'',
    'mysql'   : f'',
}



def run(cmd):
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.DEVNULL, 
                            stderr=subprocess.STDOUT)
    return proc

def stop(proc):
    print("Killing Balsam server process")
    proc.terminate()
    try: retcode = proc.wait(timeout=3)
    except subprocess.TimeoutExpired: proc.kill()

def main(db_path):

    serverinfo = ServerInfo(db_path)
    serverinfo.reset_server_address()
    server_type = serverinfo['db_type']

    db_cmd = f"BALSAM_DB_PATH={db_path} " + DB_COMMANDS[server_type].format(**serverinfo.data)
    print(f"Starting balsam DB server daemon for DB at {db_path}")

    proc = run(db_cmd)

    # On SIGINT/SIGTERM, start the clock & quit after TERM_LINGER sec
    term_start = 0
    def handle_term(signum, stack):
        global term_start
        if term_start == 0: term_start = time.time()

    # On SIGUSR1, stop immediately ("balsam server --stop" does this)
    def handle_stop(signum, stack):
        stop(proc)
        serverinfo['address'] = None
        sys.exit(0)
    
    signal.signal(signal.SIGINT, handle_term)
    signal.signal(signal.SIGTERM, handle_term)
    signal.signal(signal.SIGUSR1, handle_stop)

    while not term_start or time.time() - term_start < TERM_LINGER:
        try: 
            retcode = proc.wait(timeout=CHECK_PERIOD)
        except subprocess.TimeoutExpired: 
            pass
        else:
            print("server process stopped unexpectedly; restarting")
            serverinfo.reset_server_address()
            db_cmd = f"BALSAM_DB_PATH={db_path} " + DB_COMMANDS[server_type].format(**serverinfo.data)
            proc = run(db_cmd)
    
    stop(proc)
    serverinfo.update({'address': None})

if __name__ == "__main__":
    input_path = sys.argv[1] if len(sys.argv) == 2 else None
    db_path = resolve_db_path(input_path)
    main(db_path)
