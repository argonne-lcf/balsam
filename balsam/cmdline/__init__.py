from balsam import __version__
import click

from balsam.cmdline import init
from balsam.cmdline import ls
from balsam.cmdline import activate

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
    init.init,
    activate.activate,
    ls.ls,
]

for cmd in LOAD_COMMANDS:
    main.add_command(cmd)

if __name__ == "__main__":
    main()
