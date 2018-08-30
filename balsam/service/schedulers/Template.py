import os
from jinja2 import Template, Environment, FileSystemLoader
from django.conf import settings

class ScriptTemplate:
    def __init__(self, scheduler, JobEnv):
        schedClass = scheduler.__class__.__name__
        hostType = JobEnv.host_type
        self.JobEnv = JobEnv
        here = os.path.dirname(os.path.abspath(__file__))
        templ_path = os.path.join(here, 'templates')
        env = Environment(loader=FileSystemLoader(templ_path))
        fname = f'{hostType}.{schedClass}.tmpl'.lower()
        self._template = env.get_template(fname)

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
        balsam_env = self.JobEnv.get_balsam_env()
        conf.update(balsam_env)
        return conf
