import click
from pathlib import Path
from balsam import banner


@click.group()
def init():
    """
    Initialize a new Balsam site

    1) Create a new site with a local DB that belongs to you

         $ balsam init db ./myDB

    2) Create a new site that talks to a DB on a remote filesystem (managed somewhere else)

         $ balsam init remote-db ./myDB --credentials=./myCredentials.yml
    """
    pass


@init.command()
@click.argument("path", type=click.Path(writable=True))
@click.option("--migrate", is_flag=True, default=False)
def db(path, migrate):
    """
    Inititialize a site at PATH with local database

    1) Create a new site with a local DB that belongs to you

         $ balsam init db PATH

    2) Migrate existing local DB to a new schema (after Balsam has been updated)

        $ balsam init db --migrate PATH
    """
    from balsam.client import PostgresDjangoORMClient
    from balsam.util import postgres as pg

    site_path = path
    site_path = Path(site_path)
    pre_init_site(site_path)

    if migrate:
        client = PostgresDjangoORMClient.ensure_connection(site_path)
        verb = "migrated"
    else:
        pw_dict = pg.create_new_db(site_path)
        client = PostgresDjangoORMClient(**pw_dict)
        client.dump_yaml()
        verb = "created"

    client.run_migrations()
    banner(
        f"""
        Successfully {verb} Balsam DB at: {site_path}
        Use `balsam activate {site_path.name}` to begin working.
        """
    )
    post_init_site(site_path)


def pre_init_site(site_path):
    pass


def post_init_site(site_path):
    # site.bootstrap_settings(site_path)
    pass
