from balsam import __version__
import click
import logging
import os

from balsam.cmdline import login
from balsam.cmdline import (
    site,
    app,
    job,
    scheduler,
)

logger = logging.getLogger("balsam.cmdline")
try:
    from balsam.cmdline import server
except ModuleNotFoundError as e:
    logger.debug(f"Balsam server not installed: {e}")
    server = None

# Monkey-patch make_default_short_help: cut off after first line
_old_shorthelp = click.utils.make_default_short_help


def _new_shorthelp(help, max_length=45):
    help = help.lstrip().split("\n")[0]
    return _old_shorthelp(help, max_length)


click.utils.make_default_short_help = _new_shorthelp
click.core.make_default_short_help = _new_shorthelp


@click.group()
@click.version_option(version=__version__)
def _main():
    """
    Balsam Command Line Interface.

    Each subcommand is recursively documented; use `-h` or `--help` to get
    information for any balsam subcommand.  After using
    `balsam activate`, Balsam tab-completion is enabled in bash shells.
    """
    pass


def main():
    try:
        _main()
    except Exception as e:
        if os.environ.get("BALSAM_CLI_TRACEBACK"):
            raise
        click.echo(e)
        click.echo("    [Export BALSAM_CLI_TRACEBACK=1 to see a full stack trace]")


LOAD_COMMANDS = [
    login.login,
    login.register,
    site.site,
    app.app,
    job.job,
    scheduler.queue,
]
if server is not None:
    LOAD_COMMANDS.append(server.server)

for cmd in LOAD_COMMANDS:
    _main.add_command(cmd)

if __name__ == "__main__":
    main()
