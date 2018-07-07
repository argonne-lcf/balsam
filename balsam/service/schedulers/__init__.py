from django.conf import settings
from importlib import import_module

_schedClass = settings.SCHEDULER_CLASS.strip()
_temp = import_module('balsam.service.schedulers.'+_schedClass)
scheduler = _temp.new_scheduler()
JobEnv = scheduler.JobEnv

__all__ = ['scheduler', 'JobEnv']
