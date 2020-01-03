import os
from getpass import getuser
from datetime import datetime

from django.conf import settings
from balsam.service.schedulers.exceptions import * 
from balsam.service.schedulers import Scheduler

import logging
logger = logging.getLogger(__name__)

def new_scheduler():
    return SlurmScheduler()

class SlurmScheduler(Scheduler.Scheduler):
    SCHEDULER_VARIABLES = {
        'current_scheduler_id' : 'SLURM_JOBID',
        'num_workers'  : 'SLURM_PARTSIZE',
        'workers_str'  : 'SLURM_PARTNAME',
        'workers_file' : 'SLURM_NODEFILE',
    }
    #note the :## to explicitly truncate fields that slurm may autmatically truncate otherwise. a more robust appraoch would add a -p flag with
    #known delimeter and then split on that
    JOBSTATUS_VARIABLES = {
        'id' : 'jobid',
        'time_remaining' : 'timeleft',
        'wall_time' : 'timelimit',
        'state' : 'state',
        'queue' : 'partition',
        'nodes' : 'nodelist:30',
        'project' : 'account',
        'command' : 'command:50',
    }
    SQUEUE_EXE = settings.SCHEDULER_STATUS_EXE

    def _make_submit_cmd(self, script_path):
        exe = settings.SCHEDULER_SUBMIT_EXE # sbatch
        cwd = settings.SERVICE_PATH
        # at NERSC on Cori, -C is used to communicate node type,
        # either "haswell" or "knl"
        con = settings.JOB_CONSTRAINT
        basename = os.path.basename(script_path)
        basename = os.path.splitext(basename)[0]
        return f"{exe} --chdir {cwd} -C {con} -o {basename} {script_path}"

    def _parse_submit_output(self, submit_output):
        try: scheduler_id = int(submit_output)
        except ValueError: scheduler_id = int(submit_output.split()[-1])
        return scheduler_id

    #QSTAT_HEADER=JobID:TimeRemaining:WallTime:State:Queue:Nodes:Project:Command /usr/bin/qstat -u warndt
    #SQUEUE_FORMAT2=jobid,timeleft,timelimit,state,partition,nodelist:30,account,command:50 /usr/bin/squeue -u warndt
    def _make_status_cmd(self):
        fields = self.JOBSTATUS_VARIABLES.values()
        cmd = "SQUEUE_FORMAT2=" + ','.join(fields)
        cmd += f" {self.SQUEUE_EXE} -u {getuser()}"
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
        if len(fields) != num_expected: return {}
        stat = {}
        for i, field_name in enumerate(self.JOBSTATUS_VARIABLES.keys()):
            stat[field_name] = fields[i]
            #slurm time in forms: D-HH:MM:SS or HH:MM:SS or MM:SS
            if 'time' in field_name:
                f = fields[i]
                days = 0; hours = 0; minutes = 0; seconds = 0
                if '-' in fields[i]: 
                    days = int(f.split('-')[0])
                    f = f.split('-')[1]
                s = f.split(':')
                if len(s) == 3:
                    hours = int(s[0]); minutes = int(s[1]); seconds = int(s[2])
                elif len(s) == 2:
                    minutes = int(s[0]); seconds = int(s[1])
                else:
                    pass
                tsec = days * 86400 + hours * 3600 + minutes * 60 + seconds
                stat[field_name+"_sec"] = tsec
                tmin = days * 1440 + hours * 60 + minutes
                stat[field_name+"_min"] = tmin
        logger.debug(str(stat))
        return stat
