import getpass
import json
import logging
import os
import subprocess
import tempfile
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

import click
import dateutil.parser

from balsam.util import parse_to_utc
from balsam.schemas import BatchJobState

from .scheduler import (
    SchedulerBackfillWindow,
    SchedulerJobLog,
    SchedulerJobStatus,
    SubprocessSchedulerInterface,
    scheduler_subproc,
)

import psij
from psij import JobState as psijJobState

PathLike = Union[Path, str]

logger = logging.getLogger(__name__)


class Scheduler(SubprocessSchedulerInterface):

    def _render_submit_args(self,
        script_path: Union[Path, str], project: str, queue: str, num_nodes: int, wall_time_min: int, **kwargs: Any
    ) -> List[str]:
        job = psij.Job() # This seems to be unused by scheduler.get_submit_command
        #    def get_submit_command(self, job: Job, submit_file_path: Path) -> List[str]:
        return self.scheduler.get_submit_command( job, script_path )

    def _render_status_args(self, project: Optional[str], user: Optional[str], queue: Optional[str]) -> List[str]:                                                                                            
        return self.scheduler.get_status_command([])

    @staticmethod
    def _render_delete_args(self, job_id: Union[int, str]) -> List[str]:
        return self.scheduler.get_cancel_command( job_id )

    @staticmethod
    def _render_backfill_args() -> List[str]:
        pass

    @staticmethod
    def _parse_submit_output(self, submit_output: str) -> int:
        return self.scheduler.job_id_from_submit_output( submit_output )

#class SchedulerJobStatus(BaseModel):
    #scheduler_id: int
    #state: BatchJobState
    #queue: str
    #num_nodes: int
    #wall_time_min: int
    #project: str
    #time_remaining_min: int
    #queued_time_min: int

    def _parse_status_output(self, raw_output: str) -> Dict[int, SchedulerJobStatus]:                     
        #Balsam BatchJobStates
        #    pending_submission = "pending_submission"
        #    queued = "queued"
        #    running = "running"
        #    finished = "finished"
        #    submit_failed = "submit_failed"
        #    pending_deletion = "pending_deletion"
        #PsiJ job states
        #    The possible states are: `NEW`, `QUEUED`, `ACTIVE`, `COMPLETED`, `FAILED`, and `CANCELED`.

        logger.warn(f"psijJobState NEW = {psijJobState.NEW}")
        state_map = {
            str(psijJobState.NEW) : BatchJobState.pending_submission,
            str(psijJobState.QUEUED) : BatchJobState.queued,
            str(psijJobState.ACTIVE) : BatchJobState.running,
            str(psijJobState.COMPLETED) : BatchJobState.finished,
            str(psijJobState.FAILED) : BatchJobState.submit_failed, # FIXME: check
            str(psijJobState.CANCELED) : BatchJobState.submit_failed # FIXME: check
        }

        exit_code = 0 #FIXME: check this
        stat_dict = self.scheduler.parse_status_output( exit_code, raw_output )
        out_stat_dict = {}
        for k,psij_status in stat_dict:
            s = SchedulerJobStatus()
            s.scheduler_id = k
            s.state = state_map[psij_status.state._name]
            # these variables are needed by Balsam but not provided
            # by psij
            s.queue = "default"
            s.num_nodes = 0
            s.wall_time_min = 10
            s.project = "TBD"
            s.time_remaining_min = 10
            s.queued_time_min = 10

            out_stat_dict[int(k)] = s
        return out_stat_dict

    @staticmethod
    def _parse_backfill_output(stdout: str) -> Dict[str, List[SchedulerBackfillWindow]]:
        return dict()

    @staticmethod
    def _parse_logs(scheduler_id: int, job_script_path: Optional[PathLike]) -> SchedulerJobLog:
        pass

    @classmethod
    def get_backfill_windows(cls) -> Dict[str, List[SchedulerBackfillWindow]]:
        return []

    def get_statuses(                                                                               
        self,
        project: Optional[str] = None,
        user: Optional[str] = getpass.getuser(),
        queue: Optional[str] = None,
    ) -> Dict[int, SchedulerJobStatus]:
        stat_args = self._render_status_args(project, user, queue)
        stdout = scheduler_subproc(stat_args)
        stat_dict = self._parse_status_output(stdout)
        return out_stat_dict




class PBSScheduler(Scheduler):
    def __init__(self):
        self.scheduler = psij.JobExecutor.get_instance("pbspro")

class CobaltScheduler(Scheduler):
    def __init__(self):
        self.scheduler = psij.JobExecutor.get_instance("cobalt")

class SlurmScheduler (Scheduler):
    def __init__(self):
        self.scheduler = psij.JobExecutor.get_instance("slurm")

class LocalProcessScheduler(Scheduler):
    def __init__(self):
        self.scheduler = psij.JobExecutor.get_instance("local")


if __name__ == "__main__":
    pass
    """
    raw_output = open("qstat.out").read()
    status_dict = {}
    j = json.loads(raw_output)
    p = PBSScheduler()
    # scheduler_id = p.submit("hostname.sh","datascience","debug",1,10)
    # p.delete_job(scheduler_id)
    scheduler_id = p.submit("hostname.sh", "datascience", "debug", 1, 10)
    o = p.parse_logs(scheduler_id, "hostname")
    print("parse_logs:", o)
    o = p.get_statuses()
    for k, v in o.items():
        print(k, v)
    # not supporting this yet
    o = p.get_backfill_windows()
    print(o)
    pl = p.discover_projects()
    print(pl)
    """
