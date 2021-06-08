import logging
import os
import sys
from types import ModuleType
from typing import Any, Optional, cast

import click

from balsam import __version__
from balsam.cmdline import app, job, login, scheduler, site

server: Optional[ModuleType]

logger = logging.getLogger("balsam.cmdline")
try:
    from balsam.cmdline import server
except ModuleNotFoundError as e:
    logger.debug(f"Balsam server not installed: {e}")
    server = None

# Monkey-patch make_default_short_help: cut off after first line
_old_shorthelp = click.utils.make_default_short_help


def _new_shorthelp(help: str, max_length: int = 45) -> str:
    help = help.lstrip().split("\n")[0]
    return str(_old_shorthelp(help, max_length))


click.utils.make_default_short_help = _new_shorthelp
click.core.make_default_short_help = _new_shorthelp  # type: ignore


@click.group()
@click.version_option(version=__version__)
def _main() -> None:
    """
    Balsam Command Line Interface.

    Each subcommand is recursively documented; use `--help` to get
    information for each balsam subcommand.
    """
    pass


def main() -> None:
    try:
        _main()
    except Exception as e:
        if os.environ.get("BALSAM_CLI_TRACEBACK"):
            raise
        click.echo(f"{str(e).strip()}")
        click.echo("    [Export BALSAM_CLI_TRACEBACK=1 to see a full stack trace]")
        sys.exit(1)


LOAD_COMMANDS = [
    login.login,
    login.register,
    site.site,
    app.app,
    job.job,
    scheduler.queue,
]
if server is not None:
    LOAD_COMMANDS.append(cast(Any, server).server)

for cmd in LOAD_COMMANDS:
    _main.add_command(cmd)

if __name__ == "__main__":
    main()
