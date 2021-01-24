from balsam import __version__
import click
import logging

from balsam.cmdline import login
from balsam.cmdline import (
    site,
    app,
    job,
    queue,
    launcher,
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
def main():
    """
    Balsam Command Line Interface.

    Each subcommand is recursively documented; use `-h` or `--help` to get
    information for any balsam subcommand.  After using
    `balsam activate`, Balsam tab-completion is enabled in bash shells.
    """
    pass


LOAD_COMMANDS = [
    login.login,
    login.register,
    site.site,
    app.app,
    job.job,
    queue.queue,
    launcher.launcher,
]
if server is not None:
    LOAD_COMMANDS.append(server.server)

for cmd in LOAD_COMMANDS:
    main.add_command(cmd)

if __name__ == "__main__":
    main()
