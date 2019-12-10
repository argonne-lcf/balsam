import os
import tempfile
from pathlib import Path
import shutil
import click

from balsam.site import conf, site_index

def handover_to_bash(site_path):
    """
    Exec new /bin/bash with environ pointing at given site_path
    """
    os.environ['BALSAM_SHELL']='1'
    os.environ['OLD_PS1']=os.environ.get('PS1', '')

    bashrc_path = Path.home() / '.bashrc'
    completion_path = Path(shutil.which('balsam')).parent / 'completion.sh'

    with tempfile.NamedTemporaryFile('w+', delete=False) as rcfile:
        if bashrc_path.is_file():
            rcfile.write(f'source {bashrc_path.as_posix()}\n')

        site_abbrev = f'{site_path.parent.name}/{site_path.name}'
        rcfile.write(f'export PS1="[{site_abbrev}] $OLD_PS1"\n')

        if completion_path.is_file():
            rcfile.write(f'source {completion_path.as_posix()}')

        rcfile.flush()
        os.execvp('/bin/bash', ['/bin/bash', '--rcfile', rcfile.name])


@click.command()
@click.argument('site')
def activate(site):
    """Switch the Balsam context to the given SITE"""

    if os.environ.get('BALSAM_SHELL'):
        raise click.UsageError(
            "You cannot activate inside an existing Balsam subshell. " 
            "Please use `exit` to quit the current environment, then re-activate."
        )

    site_path = site_index.lookup(site)
    os.environ['BALSAM_SITE_PATH'] = str(site_path)
    conf.client.ensure_connection()
    handover_to_bash(Path(site))
