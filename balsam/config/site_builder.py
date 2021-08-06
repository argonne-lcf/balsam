import os
import shutil
import socket
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from uuid import UUID

import jinja2
import yaml
from pydantic import BaseModel, ValidationError

from balsam.client import RequestsClient
from balsam.platform.scheduler import SchedulerInterface

from .config import ClientSettings, InvalidSettings, Settings, SiteConfig, import_string


class SiteDefaults(BaseModel):
    class Config:
        extra = "forbid"

    title: str
    compute_node: str
    mpi_app_launcher: str
    local_app_launcher: str
    mpirun_allows_node_packing: bool
    serial_mode_startup_params: Dict[str, str]
    scheduler_class: str
    allowed_queues: Dict[str, Dict[str, int]]
    allowed_projects: List[str]
    optional_batch_job_params: Dict[str, str]
    globus_endpoint_id: Optional[UUID]


def load_default_configs(top: Optional[Path] = None) -> Dict[Path, SiteDefaults]:
    """
    Load mapping of {default_dir: default_settings_dict} from config/defaults
    """
    if top is None:
        top = Path(__file__).parent.joinpath("defaults")
    default_settings_files = top.glob("*/settings.yml")
    configs_map = {}
    for path in default_settings_files:
        site_dir = path.parent
        with open(path) as fp:
            raw = yaml.safe_load(fp)
            configs_map[site_dir] = SiteDefaults(**raw)
    return configs_map


def render_settings_file(default_settings: Dict[str, Any], template_path: Optional[Path] = None) -> str:
    """
    Generate settings.yml from the master template and a default_settings_dict
    """
    if template_path is None:
        template_path = Path(__file__).parent / "defaults/settings.tmpl.yml"
    env = jinja2.Environment(trim_blocks=True, lstrip_blocks=True)
    tmpl = env.from_string(template_path.read_text())
    return tmpl.render(default_settings)


def new_site_setup(
    site_path: Union[str, Path],
    default_site_path: Path,
    default_site_conf: SiteDefaults,
    hostname: Optional[str] = None,
    client: Optional["RequestsClient"] = None,
    settings_template_path: Optional[Path] = None,
) -> "SiteConfig":
    """
    Creates a new site directory, registers Site
    with Balsam API, and writes default settings.yml into
    Site directory
    """
    scheduler: SchedulerInterface = import_string(default_site_conf.scheduler_class)
    projects = scheduler.discover_projects()
    default_site_conf.allowed_projects = projects

    if client is None:
        client = ClientSettings.load_from_file().build_client()
    site_path = Path(site_path)
    site_path.mkdir(exist_ok=False, parents=False)

    site_id_file = site_path.joinpath(".balsam-site")

    try:
        site = client.Site.objects.create(
            hostname=socket.gethostname() if hostname is None else hostname,
            path=site_path,
        )
    except Exception:
        shutil.rmtree(site_path)
        raise
    with open(site_id_file, "w") as fp:
        fp.write(str(site.id))
    os.chmod(site_id_file, 0o440)

    with open(site_path.joinpath("settings.yml"), "w") as fp:
        settings_txt = render_settings_file(default_site_conf.dict(), template_path=settings_template_path)
        fp.write(settings_txt + "\n")

    try:
        settings = Settings.load(fp.name)
    except ValidationError as exc:
        shutil.rmtree(site_path)
        site.delete()
        raise InvalidSettings(f"Invalid Default Settings\n{exc}")

    try:
        cf = SiteConfig(site_path=site_path, settings=settings)
        for path in [cf.log_path, cf.job_path, cf.data_path]:
            path.mkdir(exist_ok=False)
        shutil.copytree(
            src=default_site_path.joinpath("apps"),
            dst=cf.apps_path,
        )
        if settings.scheduler is not None:
            job_template_path = settings.scheduler.job_template_path
        else:
            job_template_path = Path("job-template.sh")
        shutil.copy(
            src=default_site_path.joinpath(job_template_path),
            dst=cf.site_path,
        )
    except FileNotFoundError:
        site.delete()
        shutil.rmtree(site_path)
        raise
    return cf
