import shlex
import subprocess
from balsam.service.schedulers.exceptions import *
import logging
logger = logging.getLogger(__name__)

class Scheduler:
    SCHEDULER_VARIABLES = {}
    JOBSTATUS_VARIABLES = {}

    def __init__(self):
        logger.debug(f"Using scheduler class {self.__class__}")

    def submit(self, script_path):
        submit_cmd = self._make_submit_cmd(script_path)
        p = subprocess.run(submit_cmd, stdout=subprocess.PIPE,shell=True,
                             stderr=subprocess.STDOUT, encoding='utf-8')
        if p.returncode != 0: 
            raise SubmitNonZeroReturnCode(p.stdout)
        scheduler_id = self._parse_submit_output(p.stdout)
        return scheduler_id

    def _status(self):
        stat_cmd = self._make_status_cmd()
        p = subprocess.run(stat_cmd, stdout=subprocess.PIPE, shell=True,
                           stderr=subprocess.STDOUT, encoding='utf-8')
        if p.returncode != 0: 
            raise StatusNonZeroReturnCode(p.stdout)
        statinfo = self._parse_status_output(p.stdout)
        return statinfo

    def get_status(self, scheduler_id):
        scheduler_id = int(scheduler_id)
        try:
            statuses = self._status()
        except StatusNonZeroReturnCode as e:
            raise NoQStatInformation("QStat failed: {e}")
        try:
            stat = statuses[scheduler_id]
        except KeyError:
            raise NoQStatInformation(f"No status for {scheduler_id}: this might signal the job is over")
        else:
            return stat

    def status_dict(self):
        return self._status()
