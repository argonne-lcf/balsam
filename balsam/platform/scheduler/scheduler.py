from typing import Optional, Union, Dict
from pathlib import Path
from getpass import getuser
import subprocess
import os
from balsam.schemas import SchedulerJobStatus, SchedulerBackfillWindow, SchedulerJobLog

PathLike = Union[Path, str]


class JobStatus:
    pass


class BackfillWindow:
    pass


class SchedulerNonZeroReturnCode(Exception):
    pass


class SchedulerSubmitError(Exception):
    pass


def scheduler_subproc(args: list, cwd: Optional[PathLike] = None) -> str:
    p = subprocess.run(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        encoding="utf-8",
        cwd=cwd,
    )
    if p.returncode != 0:
        raise SchedulerNonZeroReturnCode(p.stdout)
    return p.stdout


class SchedulerInterface(object):
    def __init__(self):
        self._username = None
        sched_envs = self._get_envs()
        os.environ.update(sched_envs)

    @property
    def username(self) -> str:
        if self._username is None:
            self._username = getuser()
        return self._username

    def _get_envs(self) -> Dict[str, str]:
        return {}

    def submit(
        self,
        script_path: PathLike,
        project: str,
        queue: str,
        num_nodes: int,
        wall_time_min: int,
        cwd: Optional[PathLike] = None,
        **kwargs
    ) -> int:
        """
        Submit the script at `script_path` to a local job queue.
        Returns scheduler ID of the submitted job.
        """
        raise NotImplementedError

    def get_statuses(
        self, project=None, user=None, queue=None
    ) -> Dict[int, SchedulerJobStatus]:
        """
        Returns dictionary keyed on scheduler job id and a value of JobStatus for each
          job belonging to current user, project, and/or queue
        """
        raise NotImplementedError

    def delete_job(self, scheduler_id: int):
        """
        Deletes the batch job matching `scheduler_id`
        """
        raise NotImplementedError

    def get_backfill_windows(self, queue=None) -> Dict[str, SchedulerBackfillWindow]:
        """
        Returns a dictionary keyed on queue name and a value of list of
          BackfillWindow on the system for available scheduling windows
        """
        raise NotImplementedError

    def parse_logs(self, scheduler_id: int, job_script_path: str) -> SchedulerJobLog:
        """
        Reads the scheduler logs to determine job metadata like start_time and end_time
        """
        raise NotImplementedError


class SubprocessSchedulerInterface(SchedulerInterface):
    def submit(
        self,
        script_path: PathLike,
        project: str,
        queue: str,
        num_nodes: int,
        wall_time_min: int,
        cwd: Optional[PathLike] = None,
        **kwargs
    ) -> int:
        submit_args = self._render_submit_args(
            script_path, project, queue, num_nodes, wall_time_min, **kwargs
        )
        stdout = scheduler_subproc(submit_args, cwd)
        scheduler_id = self._parse_submit_output(stdout)
        return scheduler_id

    def get_statuses(
        self, project=None, user=None, queue=None
    ) -> Dict[int, SchedulerJobStatus]:
        stat_args = self._render_status_args(project, user, queue)
        stdout = scheduler_subproc(stat_args)
        stat_dict = self._parse_status_output(stdout)
        return stat_dict

    def delete_job(self, scheduler_id: int):
        delete_args = self._render_delete_args(scheduler_id)
        stdout = scheduler_subproc(delete_args)
        return stdout

    def get_backfill_windows(self, queue=None) -> Dict[str, SchedulerBackfillWindow]:
        backfill_args = self._render_backfill_args()
        stdout = scheduler_subproc(backfill_args)
        backfill_windows = self._parse_backfill_output(stdout)
        return backfill_windows

    def parse_logs(self, scheduler_id: int, job_script_path: str) -> SchedulerJobLog:
        args = self._render_parse_logs_args()
        stdout = scheduler_subproc(args)
        log_data = self._parse_logs(stdout)
        return log_data
