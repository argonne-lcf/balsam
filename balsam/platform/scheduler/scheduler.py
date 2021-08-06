import abc
import getpass
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from balsam.schemas import SchedulerBackfillWindow, SchedulerJobLog, SchedulerJobStatus

PathLike = Union[Path, str]


class SchedulerError(Exception):
    pass


class SchedulerNonZeroReturnCode(SchedulerError):
    pass


class SchedulerSubmitError(SchedulerError):
    pass


class SchedulerDeleteError(SchedulerError):
    pass


def scheduler_subproc(args: List[str], cwd: Optional[PathLike] = None) -> str:
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


class SchedulerInterface(abc.ABC):
    def __init__(self) -> None:
        pass

    @classmethod
    @abc.abstractmethod
    def submit(
        cls,
        script_path: PathLike,
        project: str,
        queue: str,
        num_nodes: int,
        wall_time_min: int,
        cwd: Optional[PathLike] = None,
        **kwargs: Any,
    ) -> int:
        """
        Submit the script at `script_path` to a local job queue.
        Returns scheduler ID of the submitted job.
        """
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def get_statuses(
        cls,
        project: Optional[str] = None,
        user: Optional[str] = getpass.getuser(),
        queue: Optional[str] = None,
    ) -> Dict[int, SchedulerJobStatus]:
        """
        Returns dictionary keyed on scheduler job id and a value of JobStatus for each
          job belonging to current user, project, and/or queue
        """
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def delete_job(cls, scheduler_id: int) -> str:
        """
        Deletes the batch job matching `scheduler_id`
        """
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def get_backfill_windows(cls) -> Dict[str, List[SchedulerBackfillWindow]]:
        """
        Returns a dictionary keyed on queue name and a value of list of
          BackfillWindow on the system for available scheduling windows
        """
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def parse_logs(cls, scheduler_id: int, job_script_path: Optional[PathLike]) -> SchedulerJobLog:
        """
        Reads the scheduler logs to determine job metadata like start_time and end_time
        """
        raise NotImplementedError

    @classmethod
    def discover_projects(cls) -> List[str]:
        """
        Get the user's allowed/preferred allocations
        """
        project = ""
        while not project:
            project = input("Enter the name of your primary allocation: ").strip()
        return [project]


class SubprocessSchedulerInterface(SchedulerInterface, abc.ABC):
    @classmethod
    def submit(
        cls,
        script_path: PathLike,
        project: str,
        queue: str,
        num_nodes: int,
        wall_time_min: int,
        cwd: Optional[PathLike] = None,
        **kwargs: Any,
    ) -> int:
        submit_args = cls._render_submit_args(script_path, project, queue, num_nodes, wall_time_min, **kwargs)
        try:
            stdout = scheduler_subproc(submit_args, cwd=cwd)
        except SchedulerNonZeroReturnCode as exc:
            raise SchedulerSubmitError(str(exc)) from exc
        scheduler_id = cls._parse_submit_output(stdout)
        return scheduler_id

    @classmethod
    def get_statuses(
        cls,
        project: Optional[str] = None,
        user: Optional[str] = getpass.getuser(),
        queue: Optional[str] = None,
    ) -> Dict[int, SchedulerJobStatus]:
        stat_args = cls._render_status_args(project, user, queue)
        stdout = scheduler_subproc(stat_args)
        stat_dict = cls._parse_status_output(stdout)
        return stat_dict

    @classmethod
    def delete_job(cls, scheduler_id: int) -> str:
        delete_args = cls._render_delete_args(scheduler_id)
        try:
            stdout = scheduler_subproc(delete_args)
        except SchedulerNonZeroReturnCode as exc:
            raise SchedulerDeleteError(str(exc)) from exc
        return stdout

    @classmethod
    def get_backfill_windows(cls) -> Dict[str, List[SchedulerBackfillWindow]]:
        backfill_args = cls._render_backfill_args()
        stdout = scheduler_subproc(backfill_args)
        backfill_windows = cls._parse_backfill_output(stdout)
        return backfill_windows

    @classmethod
    def parse_logs(cls, scheduler_id: int, job_script_path: Optional[PathLike]) -> SchedulerJobLog:
        log_data = cls._parse_logs(scheduler_id, job_script_path)
        return log_data

    @staticmethod
    @abc.abstractmethod
    def _render_submit_args(
        script_path: PathLike, project: str, queue: str, num_nodes: int, wall_time_min: int, **kwargs: Any
    ) -> List[str]:
        pass

    @staticmethod
    @abc.abstractmethod
    def _parse_submit_output(output: str) -> int:
        pass

    @staticmethod
    @abc.abstractmethod
    def _render_status_args(project: Optional[str], user: Optional[str], queue: Optional[str]) -> List[str]:
        pass

    @staticmethod
    @abc.abstractmethod
    def _parse_status_output(output: str) -> Dict[int, SchedulerJobStatus]:
        pass

    @staticmethod
    @abc.abstractmethod
    def _render_delete_args(scheduler_id: int) -> List[str]:
        pass

    @staticmethod
    @abc.abstractmethod
    def _render_backfill_args() -> List[str]:
        pass

    @staticmethod
    @abc.abstractmethod
    def _parse_backfill_output(output: str) -> Dict[str, List[SchedulerBackfillWindow]]:
        pass

    @staticmethod
    @abc.abstractmethod
    def _parse_logs(scheduler_id: int, job_script_path: Optional[PathLike]) -> SchedulerJobLog:
        pass


__all__ = [
    "SchedulerBackfillWindow",
    "SchedulerInterface",
    "SubprocessSchedulerInterface",
    "SchedulerJobLog",
    "SchedulerJobStatus",
]
