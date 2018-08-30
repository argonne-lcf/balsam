import os
import stat

from jinja2 import Template, Environment, FileSystemLoader
from django.conf import settings

import logging
logger = logging.getLogger(__name__)

class ScriptTemplate:
    def __init__(self, scheduler, JobEnv):
        schedClass = scheduler.__class__.__name__
        hostType = JobEnv.host_type
        here = os.path.dirname(os.path.abspath(__file__))
        templ_path = os.path.join(here, 'templates')
        env = Environment(loader=FileSystemLoader(templ_path))
        fname = f'{hostType}.{schedClass}.tmpl'.lower()
        self._template = env.get_template(fname)

    def _write_submit_script(self, jobscript, qlaunch):
        pk = qlaunch.pk
        fname = f'qlaunch{pk}.sh'
        path = os.path.join(settings.SERVICE_PATH, fname)
        assert not os.path.exists(path)
        with open(path, 'w') as fp:
            fp.write(jobscript)
        st = os.stat(path)
        os.chmod(path, st.st_mode | stat.S_IEXEC)
        return path

    def _qlaunch_to_dict(self, qlaunch):
        project = settings.DEFAULT_PROJECT
        if qlaunch.wf_filter:
            wf_filter = f'wf_filter={qlaunch.wf_filter}'
        else:
            wf_filter = 'consume-all'
        conf = dict(project=project,
                    queue=qlaunch.queue,
                    nodes=qlaunch.nodes,
                    time_minutes=qlaunch.wall_minutes,
                    job_mode=qlaunch.job_mode,
                    wf_filter=wf_filter,
                    serial_jobs_per_node=qlaunch.serial_jobs_per_node)
        balsam_env = get_balsam_env()
        conf.update(balsam_env)
        return conf

