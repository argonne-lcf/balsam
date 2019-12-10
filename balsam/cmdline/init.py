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
    click.echo("DB: %s" % path)
