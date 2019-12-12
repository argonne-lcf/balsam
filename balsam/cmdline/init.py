import click
import os
import tempfile
from pathlib import Path

@click.group()
def init():
    """
    Initialize a new Balsam site
    
    1) Create a new site with a local DB that belongs to you

         $ balsam init db ./myDB
    
    2) Create a new site that talks to a DB on a remote filesystem (managed somewhere else)

         $ balsam init remote-db ./myDB --credentials=./myCredentials.yml

    """
    click.echo("Init")

@init.command()
@click.argument('path', type=click.Path(writable=True))
def db(path):
    """Inititialize a site at PATH with local database"""
    site_path = Path(site_path)
    try:
        client = ORMClient.from_yaml(site_path)
    except FileNotFoundError:
        new_db = True
    else:
        new_db = False

    if new_db:
        client = pg.create_new_db(site_path, rel_db_path)
    else:
        client.ensure_connection()

    verb = 'created' if new_db else 'migrated'
    client.run_migrations(client)
    banner(f"""
        Successfully {verb} Balsam DB at: {site_path}
        Use `source balsamactivate {site_path.name}` to begin working.
        """
    )
