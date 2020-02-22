import click


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
    pass
