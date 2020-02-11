import os
from getpass import getuser
from dateutil.parser import parse as parse_time
from django.conf import settings
from balsam.service.schedulers import Scheduler
import logging
logger = logging.getLogger(__name__)


def new_scheduler():
    return SlurmScheduler()


class SlurmScheduler(Scheduler.Scheduler):
    SCHEDULER_VARIABLES = {
        'current_scheduler_id': 'SLURM_JOB_ID',
        'num_workers': 'SLURM_JOB_NUM_NODES',
        'workers_str': 'SLURM_HOSTS',
    }
    JOBSTATUS_VARIABLES = {
        'id': 'jobid',
        'time_remaining': 'timeleft',
        'wall_time': 'timelimit',
        'state': 'state',
        'queue': 'partition',
        'nodes': 'numnodes',
        'project': 'account',
        'command': 'command',
    }

    def _make_submit_cmd(self, script_path):
        cwd = settings.SERVICE_PATH
        basename = os.path.basename(script_path)
        basename = os.path.splitext(basename)[0]
        return f"sbatch --chdir {cwd} --job-name {basename} -o {basename}.out {script_path}"

    def _parse_submit_output(self, submit_output):
        try:
            scheduler_id = int(submit_output)
        except ValueError:
            scheduler_id = int(submit_output.split()[-1])
        return scheduler_id

    def _make_status_cmd(self):
        fields = self.JOBSTATUS_VARIABLES.values()
        fmt = ' '.join(fields)
        cmd = f'squeue -u {getuser()} -O "{fmt}"'
        return cmd

    def _parse_status_output(self, raw_output):
        status_dict = {}
        job_lines = raw_output.split('\n')[1:]
        for line in job_lines:
            job_stat = self._parse_job_line(line)
            if job_stat:
                id = int(job_stat['id'])
                status_dict[id] = job_stat
        return status_dict

    def _parse_job_line(self, line):
        fields = line.split()
        num_expected = len(self.JOBSTATUS_VARIABLES)
        if len(fields) != num_expected:
            return {}
        stat = {}
        for i, field_name in enumerate(self.JOBSTATUS_VARIABLES.keys()):
            stat[field_name] = fields[i]
            if 'time' in field_name:
                try:
                    t = parse_time(fields[i])
                except:
                    pass
                else:
                    tsec = t.hour*3600 + t.minute*60 + t.second
                    stat[field_name+"_sec"] = tsec
                    tmin = t.hour*60 + t.minute
                    stat[field_name+"_min"] = tmin
        logger.debug(str(stat))
        return stat
