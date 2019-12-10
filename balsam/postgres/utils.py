from django import db
from django.core.management import call_command
import yaml

from balsam.client.orm_client import ORMClient
import .controller as pg
from .dirlock import DirLock

def create_new_db(site_path, rel_db_path='balsamdb'):
    """
    Create & start a new balsamdb inside a site directory
    Returns ORMClient to new server
    """
    pg.version_check()

    site_path=Path(site_path).absolute()
    db_path = site_path / rel_db_path

    superuser, password = pg.init_db_cluster(db_path)
    host, port = pg.identify_hostport()
    pg.mutate_conf_port(db_path, port)

    client = ORMClient(
        user=superuser,
        password=password,
        site_id=0,
        site_path=site_path,
        host=host,
        port=port,
        rel_db_path=rel_db_path,
    )
    client.dump_yaml()
    
    pg.start_db(db_path)
    pg.create_database(
        new_dbname=client.db_name,
        **client.dict_config()
    )
    return client

def run_migrations(client):
    """
    Run migrations through an ORMClient
    """
    call_command('migrate', interactive=True, verbosity=2)
    test_connection(client, raises=True)

def test_connection(client, raises=False):
    """
    Returns True if client can reach Job DB.
    If raises=True, will not mask exception
    """
    db.connections.close_all()
    Job = client.DJANGO_MODELS['Job']
    try:
        c = Job.objects.count()
    except db.OperationalError:
        if raises: raise
        return False
    else:
        return True
        
def ensure_connection(client):
    """
    Test connection; restarting server if necessary and owner
    """
    if test_connection(client):
        banner("Connected to already running Balsam DB server!")
        return

    if not client.rel_db_path:
        raise RuntimeError(
            f"Cannot reach DB at {client.host}:{client.port}.\n"
            f"Please ask owner to restart the DB, then update the "
            f"host and port in {client.INFO_FILENAME}"
        )
    
    db_path = Path(client.site_path) / Path(client.rel_db_path)
    host, port = pg.identify_hostport()
    pg.mutate_conf_port(db_path, port)
    client.set_host_port(host, port)
    client.dump_yaml()
    
    pg.start_db(db_path)
    test_connection(client, raises=True)


def init(site_path, rel_db_path='balsamdb'):
    """
    Create a new PG DB cluster in `site_path`/`rel_db_path`.
    Or, if it already exists, apply migrations to the DB.
    """
    site_path = Path(site_path)
    try:
        client = ORMClient.from_yaml(site_path)
    except FileNotFoundError:
        new_db = True
    else:
        new_db = False

    if new_db:
        client = create_new_db(site_path, rel_db_path)
    else:
        ensure_connection(client)

    verb = 'created' if new_db else 'migrated'
    run_migrations(client)
    banner(f"""
        Successfully {verb} Balsam DB at: {site_path}
        Use `source balsamactivate {site_path.name}` to begin working.
        """
    )

def start_db(site_path):
    """
    Try to start the DB under `site_path` if it's not already running
    """
    site_path = Path(site_path)
    with DirLock(site_path):
        client = ORMClient.from_yaml(site_path)
        ensure_connection(client)

def create_user(username, owner_site_path):
    """
    Create a new Postgres user with `username` and auto-generated
    token for the database under `owner_site_path`.  A file
    containing the DB credentials is written for transfer to the new user
    """
    owner_client = ORMClient.from_yaml(owner_site_path)
    
    token = pg.make_token()
    pg.add_user(
        username,
        token,
        owner_client.host,
        owner_client.port,
        owner_client.user,
        owner_client.password,
    )
    with open(f'{username}-info.yml', 'w') as fp:
        yaml.dump(
            dict(
                user=username,
                password=token,
                host=owner_client.host,
                port=owner_client.port,
            )
        )
    banner(
        f"Created new Postgres user: data in {fp.name}.\n"
        f"Protect this file in transit to {username}! "
        f"It contains the token necessary to reach the DB. "
        f"The new user can now create Balsam endpoints using the command: \n"
        f"`balsam init --remote-db {fp.name}`"
    )
