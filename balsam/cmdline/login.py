import click
from balsam.config import ClientSettings


@click.command()
@click.option("-a", "--address", prompt="Balsam server address")
@click.option("-u", "--username", prompt="Balsam username")
def login(address, username):
    """
    Set client information and authenticate to server
    """
    settings = ClientSettings(api_root=address, username=username)
    client = settings.build_client()
    update_fields = client.interactive_login()
    if update_fields:
        data = settings.dict()
        data.update(update_fields)
        updated_settings = ClientSettings(**data)
        updated_settings.save_to_home()
