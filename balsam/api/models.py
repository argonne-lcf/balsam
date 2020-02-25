import jinja2
import jinja2.meta
import shlex

import pathlib
from datetime import datetime
from typing import Union, List, Tuple
from pydantic import validator
from .base_model import BalsamModel
from .query import Manager


class Job(BalsamModel):
    name: str
    workflow: str
    num_nodes: int
    cpu_affinity = "depth"


class JobManager(Manager):
    model_class = Job


class SiteStatus(BalsamModel):
    num_nodes: int = 0
    num_idle_nodes: int = 0
    num_busy_nodes: int = 0
    num_down_nodes: int = 0
    backfill_windows: List[Tuple[int, int]] = [(0, 0)]
    queued_jobs: List[Tuple[int, int, str]] = [(0, 0, "")]


class Site(BalsamModel):
    pk: Union[int, None] = None
    hostname: str
    path: pathlib.Path
    last_refresh: datetime = datetime.utcnow
    status: SiteStatus
    apps: List[str] = [""]


class SiteManager(Manager):
    model_class = Site


class App(BalsamModel):
    command_template: str = "echo Hello, {{name}}!"

    @validator("command_template")
    def exec_exists(cls, v):
        split_cmd = v.strip().split()
        cmd = " ".join(split_cmd)
        exe = split_cmd[0]
        if not exe.isalnum():
            raise RuntimeError("invalid")
        return cmd

    def __init__(self):
        self.command_template = " ".join(self.command_template.strip().split())
        ctx = jinja2.Environment().parse(self.command_template)
        self.parameters = jinja2.meta.find_undeclared_variables(ctx)

    def render_command(self, arg_dict):
        """
        Args:
            - arg_dict: value for each required parameter
        Returns:
            - str: shell command with safely-escaped parameters
            Use shlex.split() to split the result into args list.
            Do *NOT* use string.join on the split list: unsafe!
        """
        diff = self.parameters.difference(arg_dict.keys())
        if diff:
            raise ValueError(f"Missing required args: {diff}")

        sanitized_args = {
            key: shlex.quote(str(arg_dict[key])) for key in self.parameters
        }
        return jinja2.Template(self.command_template).render(sanitized_args)

    def preprocess(self):
        pass

    def postprocess(self):
        pass

    def shell_preamble(self):
        pass

    def handle_timeout(self):
        self.job.rerun()

    def handle_error(self):
        self.job.fail()
