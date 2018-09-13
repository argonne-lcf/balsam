import os
import sys
import subprocess
from importlib.util import find_spec

from jinja2 import Template, Environment, FileSystemLoader
from django.conf import settings
import logging
logger = logging.getLogger(__name__)

class ScriptTemplate:
    def __init__(self, template_top, template_path):
        templ_path = os.path.join(template_top, template_path)
        template_dir = os.path.dirname(templ_path)
        template_name = os.path.basename(templ_path)
        env = Environment(loader=FileSystemLoader(template_dir))
        self._template = env.get_template(template_name)
        logger.debug(f"Loaded job template at {templ_path}")

    def render(self, qlaunch):
        conf = self.qlaunch_to_dict(qlaunch)
        return self._template.render(conf)

    def qlaunch_to_dict(self, qlaunch):
        if qlaunch.project:
            project = qlaunch.project
        else:
            project = settings.DEFAULT_PROJECT
        if qlaunch.wf_filter:
            wf_filter = f'wf-filter={qlaunch.wf_filter}'
        else:
            wf_filter = 'consume-all'
        conf = dict(project=project,
                    queue=qlaunch.queue,
                    nodes=qlaunch.nodes,
                    time_minutes=qlaunch.wall_minutes,
                    job_mode=qlaunch.job_mode,
                    wf_filter=wf_filter,
                    )
        balsam_env = self.get_balsam_env()
        conf.update(balsam_env)
        return conf

    @staticmethod
    def get_balsam_env():
        conda_env = os.environ.get('CONDA_PREFIX')
        if conda_env:
            activate_path = os.path.join(conda_env, 'bin', 'activate')
            conda_path = conda_env
            python_path = None
        else:
            activate_path = None
            conda_path = None
            python_path = os.path.dirname(sys.executable)

        balsam_lib = os.path.dirname(find_spec('balsam').origin)
        p = subprocess.run('which balsam', shell=True, stdout=subprocess.PIPE, encoding='utf-8')
        balsam_bin = os.path.dirname(p.stdout)
        site_top = os.path.dirname(os.path.dirname(find_spec('django').origin))
        balsam_db_path = os.environ['BALSAM_DB_PATH']
        return dict(conda_path=conda_path,
                    activate_path=activate_path,
                    python_path=python_path,
                    balsam_lib=balsam_lib,
                    balsam_bin=balsam_bin,
                    site_top=site_top,
                    balsam_db_path=balsam_db_path)
