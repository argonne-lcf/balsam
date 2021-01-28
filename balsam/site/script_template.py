import logging
import os
import shutil
import sys
from importlib.util import find_spec
from pathlib import Path

from jinja2 import Template

logger = logging.getLogger(__name__)


class ScriptTemplate:
    def __init__(self, template_path):
        """
        Wraps a batch job script template located at `template_path`
        """
        with open(template_path) as fp:
            self._template = Template(fp.read())
        logger.debug(f"Loaded job template at {template_path}")

    @classmethod
    def discover(cls, directory):
        paths = Path(directory).glob("**/*.tmpl")
        templates = {}
        for p in paths:
            templates[p.name] = cls(p)
        return templates

    def render(
        self,
        project,
        queue,
        num_nodes,
        wall_time_min,
        job_mode,
        filter_tags=None,
        partitions=None,
        **kwargs,
    ):
        """
        Returns string contained filled batch job script
        """
        conf = dict(
            project=project,
            queue=queue,
            num_nodes=num_nodes,
            wall_time_min=wall_time_min,
            job_mode=job_mode,
            filter_tags=filter_tags,
            partitions=partitions,
            optional_params=kwargs,
        )
        conf["balsam_site_path"] = os.environ["BALSAM_SITE_PATH"]
        conf["balsam_bin"] = self.locate_balsam()
        conf["pg_bin"] = self.locate_postgres()
        conf["launcher_cmd"] = self.launcher_entrypoint()
        return self._template.render(conf)

    @staticmethod
    def locate_balsam():
        balsam_bin = shutil.which("balsam") or sys.executable
        return Path(balsam_bin).parent

    @staticmethod
    def locate_postgres():
        pg_bin = shutil.which("pg_ctl") or os.environ.get("POSTGRES_BIN")
        return Path(pg_bin).parent if pg_bin else None

    @staticmethod
    def launcher_entrypoint():
        launcher_path = find_spec("balsam.cmdline.launcher").origin
        if launcher_path is None:
            raise RuntimeError("Could not locate balsam.cmdline.launcher module")
        path = Path(launcher_path).resolve()
        return f"{sys.executable} {path}"
