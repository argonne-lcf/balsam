import logging
import os
import shutil
import sys
from importlib.util import find_spec
from pathlib import Path
from typing import Dict, Optional, Union

from jinja2 import Template

from balsam.schemas import JobMode

PathLike = Union[str, Path]
logger = logging.getLogger(__name__)


class ScriptTemplate:
    def __init__(self, template_path: PathLike) -> None:
        """
        Wraps a batch job script template located at `template_path`
        """
        with open(template_path) as fp:
            self._template = Template(fp.read())
        logger.debug(f"Loaded job template at {template_path}")

    def render(
        self,
        project: str,
        queue: str,
        num_nodes: int,
        wall_time_min: int,
        job_mode: JobMode,
        filter_tags: Optional[Dict[str, str]] = None,
        partitions: Optional[str] = None,
        **kwargs: str,
    ) -> str:
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
    def locate_balsam() -> Path:
        balsam_bin = shutil.which("balsam") or sys.executable
        return Path(balsam_bin).parent

    @staticmethod
    def locate_postgres() -> Optional[Path]:
        pg_bin = shutil.which("pg_ctl") or os.environ.get("POSTGRES_BIN")
        return Path(pg_bin).parent if pg_bin else None

    @staticmethod
    def launcher_entrypoint() -> str:
        spec = find_spec("balsam.cmdline.launcher")
        if spec is None:
            raise RuntimeError("Could not locate balsam.cmdline.launcher module")
        launcher_path = spec.origin
        if launcher_path is None:
            raise RuntimeError("Could not locate balsam.cmdline.launcher module")
        path = Path(launcher_path).resolve()
        return f"{sys.executable} {path}"
