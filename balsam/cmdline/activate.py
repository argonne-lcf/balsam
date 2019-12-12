import os
import tempfile
from pathlib import Path
import shutil
import click

from balsam.site import conf

def handover_to_bash(site_path):
    """
    Exec new /bin/bash with environ pointing at given site_path
    """
    os.environ['BALSAM_SHELL']='1'
    os.environ['OLD_PS1']=os.environ.get('PS1', '')
    completion_path = Path(shutil.which('balsam')).parent / 'completion.sh'

    with tempfile.NamedTemporaryFile('w+', delete=False) as rcfile:
        site_abbrev = f'{site_path.parent.name}/{site_path.name}'
        rcfile.write(f'export PS1="[{site_abbrev}] $OLD_PS1"\n')
        if completion_path.is_file():
            rcfile.write(f'source {completion_path.as_posix()}')
        rcfile.flush()
        os.execvp('/bin/bash', ['/bin/bash', '--rcfile', rcfile.name])

def validate_site_path(ctx, param, value):
    path = Path(value).expanduser().resolve()
    if path.joinpath('settings.py').is_file():
        return path
    raise click.BadParameter(f'{path} is not a valid Balsam site directory')

@click.command()
@click.argument('site-path', type=click.Path(writable=True), callback=validate_site_path)
def activate(site_path):
    """Switch the Balsam context to the given SITE-PATH"""

    if os.environ.get('BALSAM_SHELL') is not None:
        raise click.UsageError(
            "You cannot activate inside an existing Balsam subshell. " 
            "Please use `exit` to quit the current environment, then re-activate."
        )

    os.environ['BALSAM_SITE_PATH'] = str(site_path)
    conf.client.ensure_connection()
    conf.ensure_service()
    handover_to_bash(site_path)
