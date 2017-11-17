from django.conf import settings
from importlib import import_module

scheduler_class = settings.BALSAM_SCHEDULER_CLASS

def _dummy(): pass

if scheduler_class:
    try:
        _temp = import_module('balsam.schedulers.'+scheduler_class)
        submit = _temp.submit
        get_job_status = _temp.get_job_status
        get_environ = _temp.get_environ
else:
    submit = _dummy
    get_job_status = _dummy
    get_environ = _dummy
