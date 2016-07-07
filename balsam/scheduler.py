
from django.conf import settings

_temp = __import__('schedulers.'+settings.BALSAM_SCHEDULER_CLASS, globals(), locals(), 
                   ['submit','get_job_status','postprocess'], -1)
submit = _temp.submit
#status = _temp.status
#get_queue_state = _temp.get_queue_state
#presubmit = _temp.presubmit
postprocess = _temp.postprocess
get_job_status = _temp.get_job_status


