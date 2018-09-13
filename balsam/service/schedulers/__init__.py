from django.conf import settings
from importlib import import_module
from balsam.service.schedulers import JobEnvironment, JobTemplate

_schedClass = settings.SCHEDULER_CLASS.strip()
_temp = import_module('balsam.service.schedulers.'+_schedClass)
scheduler = _temp.new_scheduler()
JobEnv = JobEnvironment.JobEnvironment(scheduler)

template_path = settings.BALSAM_HOME
template_name = settings.JOB_TEMPLATE
script_template = JobTemplate.ScriptTemplate(template_path, template_name)

__all__ = ['scheduler', 'JobEnv', 'script_template']
