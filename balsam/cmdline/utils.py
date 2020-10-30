import click
from balsam.config import SiteConfig


def list_to_dict(arg_list):
    return dict(arg.split("=") for arg in arg_list)


def validate_tags(ctx, param, value):
    try:
        return list_to_dict(value)
    except ValueError:
        raise click.BadParameter("needs to be in format KEY=VALUE")


def load_site_config() -> SiteConfig:
    try:
        cf = SiteConfig()
    except ValueError:
        raise click.BadParameter(
            "Cannot perform this action outside the scope of a Balsam Site. "
            "Please navigate into a Balsam site directory, or set "
            "BALSAM_SITE_PATH"
        )
    return cf
