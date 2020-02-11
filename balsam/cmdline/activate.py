import os
import tempfile
from pathlib import Path
import shutil
import click


def handover_to_bash(site_path):
    """
    Exec new /bin/bash with environ pointing at given site_path
    """
    os.environ["BALSAM_SHELL"] = "1"
    os.environ["OLD_PS1"] = os.environ.get("PS1", "")
    completion_path = Path(shutil.which("balsam")).parent / "completion.sh"

    with tempfile.NamedTemporaryFile("w+", delete=False) as rcfile:
        site_abbrev = f"{site_path.parent.name}/{site_path.name}"
        rcfile.write(f'export PS1="[{site_abbrev}] $OLD_PS1"\n')
        if completion_path.is_file():
            rcfile.write(f"source {completion_path.as_posix()}")
        rcfile.flush()
        os.execvp("/bin/bash", ["/bin/bash", "--rcfile", rcfile.name])


def validate_site_path(ctx, param, value):
    path = Path(value).expanduser().resolve()
    if path.joinpath("settings.py").is_file():
        return path
    raise click.BadParameter(f"Invalid Balsam site: {path} is missing settings.py")


@click.group()
def activate():
    """
    Start Balsam services and set environment variables

    1) Ensure service is running without altering the shell environment:

        $ balsam activate service ./my-site

    2) Ensure service is running, then drop into a child Bash shell
    with Balsam environment (BALSAM_SITE_PATH and tab-completion) exported:

        $ balsam activate shell ./my-site

    3) Ensure service is up and running, then export Balsam environment to the
    current shell:

        $ source balsamactivate ./my-site
    """
    pass


@activate.command("service")
@click.argument(
    "site-path", type=click.Path(writable=True), callback=validate_site_path
)
def start_service(site_path):
    """
    Ensure that Balsam service is running for the local SITE-PATH
    """
    from balsam.client import ClientAPI
    from balsam.site import service

    os.environ["BALSAM_SITE_PATH"] = str(site_path)
    ClientAPI.ensure_connection(site_path)
    service.ensure_service()


@activate.command()
@click.argument(
    "site-path", type=click.Path(writable=True), callback=validate_site_path
)
@click.pass_context
def shell(ctx, site_path):
    """
    Ensure Balsam service is running at SITE-PATH and drop into a Bash subshell
    """
    if os.environ.get("BALSAM_SHELL") is not None:
        raise click.UsageError(
            "You cannot start a nested shell inside the existing Balsam subshell. "
            "Please use `exit` to quit the current environment, then re-activate."
        )

    ctx.forward(start_service)
    handover_to_bash(site_path)
