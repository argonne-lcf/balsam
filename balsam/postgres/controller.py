import secrets
import subprocess
import shutil
import tempfile
from pathlib import Path
import logging
import psycopg2

MIN_VERSION = (10,0,0)
POSTGRES_MSG = \
f'''You need PostgreSQL {'.'.join(map(str, MIN_VERSION))} or newer.
Ensure pg_ctl is in the search PATH, and double-check version with pg_ctl --version.
You can easily grab the Postgres binaries at:
https://www.enterprisedb.com/download-postgresql-binaries
'''

logger = logging.getLogger(__name__)

def make_token() -> str:
    return secrets.token_urlsafe()

def make_token_file():
    with tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8') as fp:
        token = make_token()
        fp.write(token)
        tmp_pwfile = fp.name
    return token, tmp_pwfile

def identify_free_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('', 0))
    port = int(sock.getsockname()[1])
    sock.close()
    return port

def identify_hostport():
    host = socket.gethostname()
    port = identify_free_port()
    return (host, port)

# *******************************
# Functions that use a subprocess
# *******************************
def version_check():
    p = subprocess.run(
        'pg_ctl --version',
        shell=True,
        encoding='utf-8',
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=True
    )
    stdout = p.stdout.strip()
    pattern = re.compile(r'(\d+\.)?(\d+\.)?(\*|\d+)$')
    match = pattern.search(stdout).group()
    major, minor, *rest = match.split('.')
    if rest and int(rest[0])>0:
        version_info = (int(major), int(minor), int(rest[0]))
    else:
        version_info = (int(major), int(minor))
    if version_info >= MIN_VERSION:
        return True
    raise RuntimeError(
        f"PostgreSQL {version_info} too old. {POSTGRES_MSG}"
    )

def mutate_conf_port(db_path: str, port: int) -> None:
    settings_path = Path(db_path) / Path('postgresql.conf')
    if not settings_path.exists():
        raise ValueError(f'nonexistant {settings_path}')

    new_port = f"port={port}"
    cmd = f"sed -i -E 's/^[[:space:]]*port.*/{new_port}/g' {settings_path}"
    subprocess.run(cmd, shell=True, check=True)

def init_db_cluster(db_path, superuser='postgres'):
    db_path = Path(db_path)
    if db_path.exists():
        raise ValueError(f'{db_path} already exists')

    token, tmp_pwfile = make_token_file()

    res = subprocess.run(
        f'initdb -D {db_path} --auth scram-sha-256 -U {superuser} --pwfile {tmp_pwfile}', 
        shell=True,
        check=True
    )
    shutil.move(tmp_pwfile, db_path/"pwfile")

    with open(db_path/'postgresql.conf', 'a') as fp:
        fp.write("listen_addresses = '*' # appended from balsam init\n")
        fp.write('port=0 # appended from balsam init\n')
        fp.write("password_encryption = 'scram-sha-256'\n")
        fp.write('max_connections=128 # appended from balsam init\n')
        fp.write('shared_buffers=2GB # appended from balsam init\n')
        fp.write('synchronous_commit=off # appended from balsam init\n')
        fp.write('wal_writer_delay=400ms # appended from balsam init\n')

        # logging
        fp.write('logging_collector=on # appended from balsam init\n')
        fp.write('log_min_duration_statement=0 # appended from balsam init\n')
        fp.write('log_connections=on # appended from balsam init\n')
        fp.write('log_duration=on # appended from balsam init\n')

    with open(db_path/'pg_hba.conf', 'a') as fp:
        fp.write('host all all ::1/128 scram-sha-256\n')
        fp.write('host all all 0.0.0.0/0 scram-sha-256\n')
    return superuser, token


def start_db(db_path: str) -> None:
    log_path = Path(db_path) /  'postgres.log'
    start_cmd = f"pg_ctl -w start -D {db_path} -l {log_path} --mode=smart"
    logger.info(start_cmd)
    proc = subprocess.run(start_cmd, shell=True, check=True)

def stop_db(db_path: str) -> None:
    stop_cmd = f"pg_ctl -w stop -D {db_path} --mode=smart"
    res = subprocess.run(stop_cmd, shell=True)
    if res.returncode != 0:
        raise RuntimeError("Could not send stop signal; is the DB running on a different host?")


# ***********************************
# Functions that use a psycopg cursor
# ***********************************
class PgCursor:
    def __init__(self, host, port, user, password, dbname='balsam', autocommit=False):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.dbname = dbname
        self.autocommit = autocommit

    def __enter__(self):
        self.conn = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )
        self.conn.autocommit = self.autocommit
        self.cursor = self.conn.cursor()
        return self.cursor

    def __exit__(self, exc_type, value, traceback):
        if not self.autocommit:
            if exc_type is None:
                self.conn.commit()
            else:
                self.conn.rollback()
        self.cursor.close()
        self.conn.close()

def create_database(new_dbname, host, port, user, password, **kwargs):
    with PgCursor(host, port, user, password, dbname='', autocommit=True) as cur:
        cur.execute(f"""
        CREATE DATABASE {new_dbname};
        """)

def connections_list(host, port, user, password, **kwargs):
    with PgCursor(host, port, user, password) as cur:
        cur.execute("""
        SELECT pid,application_name,usename,state,substr(query, 1, 60) \
        FROM pg_stat_activity WHERE datname = 'balsam';
        """)
        return cur.fetchall()

def add_user(new_user, new_password, host, port, user, password, **kwargs):
    with PgCursor(host, port, user, password) as cur:
        cur.execute(f'''
        CREATE ROLE {new_user} WITH LOGIN PASSWORD '{new_password}'; \
        grant all privileges on database balsam to {new_user}; \
        GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {new_user}; \
        GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO {new_user};
        ''')

def drop_user(deleting_user, host, port, user, password, **kwargs):
    with PgCursor(host, port, user, password) as cur:
        cur.execute(f'''
        REVOKE ALL privileges on all tables in schema public FROM {deleting_user}; \
        REVOKE ALL privileges on all sequences in schema public FROM {deleting_user}; \
        REVOKE ALL privileges on database balsam FROM {deleting_user}; \
        DROP ROLE {deleting_user};
        ''')

def list_users(host, port, user, password, **kwargs):
    with PgCursor(host, port, user, password) as cur:
        cur.execute('SELECT usename FROM pg_catalog.pg_user;')
        return cur.fetchall()
