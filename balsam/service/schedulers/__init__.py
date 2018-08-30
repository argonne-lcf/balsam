from django.conf import settings
from importlib import import_module
from balsam.service.schedulers import JobEnvironment, Template

_schedClass = settings.SCHEDULER_CLASS.strip()
_temp = import_module('balsam.service.schedulers.'+_schedClass)
scheduler = _temp.new_scheduler()
JobEnv = JobEnvironment.JobEnvironment(scheduler)
script_template = Template.ScriptTemplate(scheduler, JobEnv)

__all__ = ['scheduler', 'JobEnv', 'script_template']
