from pydantic import BaseModel, PostgresDsn, AnyHttpUrl, ValidationError
from typing import Union
import yaml
import click
from balsam.config import ClientSettings


class AddressParser(BaseModel):
    address: Union[PostgresDsn, AnyHttpUrl]


@click.command()
@click.option("-a", "--address")
@click.option("-f", "--file")
def login(address, file):
    """
    Set client information and authenticate to server
    """
    if address and file:
        raise click.BadParameter("Provide either address or file, not both")
    elif address:
        try:
            address = AddressParser(address=address).address
        except ValidationError as e:
            raise click.BadParameter(str(e))
        settings = ClientSettings.from_url(address)
        settings.save_to_home()
    elif file:
        with open(file) as fp:
            data = yaml.safe_load(fp)
        try:
            settings = ClientSettings(**data)
        except ValidationError as e:
            raise click.BadParameter(str(e))
        settings.save_to_home()
    else:
        try:
            settings = ClientSettings.load_from_home()
        except FileNotFoundError:
            raise click.BadParameter(
                "Login with --address or --file argument to provide server info"
            )

    client = settings.build_client()
    update_fields = client.interactive_login()
    if update_fields:
        data = settings.dict()
        data.update(update_fields)
        updated_settings = ClientSettings(**data)
        updated_settings.save_to_home()
