import getpass

import click

from balsam.client import NotAuthenticatedError, RequestsClient, urls
from balsam.config import ClientSettings


def is_auth() -> bool:
    """
    Returns True only if an access token is stored and working.
    """
    try:
        settings = ClientSettings.load_from_file()
    except NotAuthenticatedError:
        return False

    client = settings.build_client()
    return client._authenticated


@click.command()
@click.option("-u", "--url", default="https://balsam-dev.alcf.anl.gov", help="Balsam server address")
@click.option("-f", "--force", is_flag=True, default=False, help="Force redo, even if logged in")
def login(url: str, force: bool) -> None:
    """
    Set client information and authenticate to server
    """
    if not force and is_auth():
        click.echo("You already have a good login stored.")
        click.echo("To force login, try again with the --force option.")
        return

    url = url.strip()
    if not url.startswith("http"):
        url = "https://" + url
    click.echo(f"Logging into {url}")

    client = RequestsClient.discover_supported_client(url)
    settings = ClientSettings(api_root=url, client_class=type(client))

    update_fields = client.interactive_login()
    if update_fields:
        data = settings.dict()
        data.update(update_fields)
        updated_settings = ClientSettings(**data)
        updated_settings.save_to_file()


@click.command()
@click.option("-a", "--address", prompt="Balsam server address", help="Balsam server address")
@click.option("-u", "--username", prompt="Balsam username", help="Balsam username")
def register(address: str, username: str) -> None:
    """
    Register a new user account with Balsam server
    """
    settings = ClientSettings(api_root=address)
    client = settings.build_client()
    password = getpass.getpass("Password:")
    conf_password = getpass.getpass("Confirm Password:")
    if password != conf_password:
        raise click.BadParameter("Passwords must match")

    resp = client.post(urls.PASSWORD_REGISTER, username=username, password=password, authenticating=True)
    click.echo(f"Registration success! {resp}")
