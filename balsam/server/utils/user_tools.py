import click
import yaml

from balsam.client.encoders import jsonable_encoder
from balsam.schemas import UserOut
from balsam.server.auth.token import create_access_token
from balsam.server.models import get_session
from balsam.server.models.crud.users import create_user, get_user_by_username


@click.group()
def cli() -> None:
    pass


@cli.command()
@click.argument("username")
def new_user(username: str) -> None:
    db = next(get_session())
    userout = create_user(db, username, password=None)
    db.commit()
    print("Created user", userout)


@cli.command()
@click.argument("username")
def make_token(username: str) -> None:
    db = next(get_session())
    from_db = get_user_by_username(db, username)
    user_out = UserOut(id=from_db.id, username=from_db.username)
    jwt, expiry = create_access_token(user_out)
    s = yaml.dump(
        jsonable_encoder(
            {
                "api_root": "https://balsam-dev.alcf.anl.gov",
                "username": username,
                "client_class": "balsam.client.BasicAuthRequestsClient",
                "token": jwt,
                "token_expiry": expiry,
                "connect_timeout": 6.2,
                "read_timeout": 120.0,
                "retry_count": 3,
            }
        )
    )
    print(s)


if __name__ == "__main__":
    cli()
