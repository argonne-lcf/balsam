import click
import getpass
from balsam.config import ClientSettings
from balsam.client import NotAuthenticatedError


@click.command()
@click.option("-a", "--address")
@click.option("-u", "--username")
def login(address, username):
    """
    Set client information and authenticate to server
    """
    try:
        settings = ClientSettings.load_from_home()
    except NotAuthenticatedError:
        address = click.prompt("Balsam server address")
        username = click.prompt("Balsam username")
        if not address.startswith("http"):
            address = "https://" + address
        settings = ClientSettings(api_root=address, username=username)

    click.echo(f"Logging in as {settings.username} to {settings.api_root}")
    client = settings.build_client()
    update_fields = client.interactive_login()
    if update_fields:
        data = settings.dict()
        data.update(update_fields)
        updated_settings = ClientSettings(**data)
        updated_settings.save_to_home()


@click.command()
@click.option("-a", "--address", prompt="Balsam server address")
@click.option("-u", "--username", prompt="Balsam username")
def register(address, username):
    """
    Register a new user account with Balsam server
    """
    settings = ClientSettings(api_root=address, username=username)
    client = settings.build_client()
    password = getpass.getpass("Password:")
    conf_password = getpass.getpass("Confirm Password:")
    if password != conf_password:
        raise click.BadParameter("Passwords must match")

    resp = client.post("users/register", username=username, password=password)
    if resp.status_code == 201:
        click.echo(f"Registration success! {resp.json()}")
    else:
        click.echo(f"Registration failed!\n {resp.text()}")
