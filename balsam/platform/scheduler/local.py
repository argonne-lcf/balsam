import getpass
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import psutil  # type: ignore

from .scheduler import SchedulerBackfillWindow, SchedulerInterface, SchedulerJobLog, SchedulerJobStatus

PathLike = Union[str, Path]


class LocalProcessScheduler(SchedulerInterface):
    _state_map = {
        psutil.STATUS_ZOMBIE: "finished",
        psutil.STATUS_DEAD: "finished",
    }

    _subprocesses: List["subprocess.Popen[bytes]"] = []

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
    ) -> Union[str, int]:
        """
        Submit the script at `script_path` to a local job queue.
        Returns scheduler ID of the submitted job.
        """
        outfile = Path(script_path).resolve().with_suffix(".output")
        with open(outfile, "wb") as fp:
            p = subprocess.Popen(
                f"bash {script_path}",
                shell=True,
                stdout=fp,
                stderr=subprocess.STDOUT,
            )
        cls._subprocesses.append(p)
        return p.pid

    @classmethod
    def get_statuses(
        cls,
        project: Optional[str] = None,
        user: Optional[str] = getpass.getuser(),
        queue: Optional[str] = None,
    ) -> Dict[Union[int, str], SchedulerJobStatus]:
        """
        Returns dictionary keyed on scheduler job id and a value of JobStatus for each
          job belonging to current user, project, and/or queue
        """
        results: Dict[Union[int, str], SchedulerJobStatus] = {}
        for p in psutil.process_iter(attrs=["pid", "username", "status"]):
            if p.info["username"] == user:
                pid = int(p.info["pid"])
                status = p.info["status"]
                results[pid] = SchedulerJobStatus(
                    scheduler_id=pid,
                    state=cls._state_map.get(status, "running"),
                    queue="local",
                    num_nodes=1,
                    wall_time_min=0,
                    project="local",
                    time_remaining_min=1000,
                )
        cls._subprocesses = [p for p in cls._subprocesses if p.poll() is None]
        return results

    @classmethod
    def delete_job(cls, scheduler_id: int) -> str:
        """
        Deletes the batch job matching `scheduler_id`
        """
        if psutil.pid_exists(scheduler_id):
            psutil.Process(scheduler_id).terminate()
        return str(scheduler_id)

    @classmethod
    def get_backfill_windows(cls) -> Dict[str, List[SchedulerBackfillWindow]]:
        """
        Returns a dictionary keyed on queue name and a value of list of
          BackfillWindow on the system for available scheduling windows
        """
        return {}

    @classmethod
    def parse_logs(cls, scheduler_id: int, job_script_path: str) -> SchedulerJobLog:
        """
        Reads the scheduler logs to determine job metadata like start_time and end_time
        """
        return SchedulerJobLog()
