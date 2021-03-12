import datetime
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

from .scheduler import (
    SchedulerBackfillWindow,
    SchedulerJobLog,
    SchedulerJobStatus,
    SchedulerNonZeroReturnCode,
    SubprocessSchedulerInterface,
    scheduler_subproc,
)

PathLike = Union[Path, str]
logger = logging.getLogger(__name__)


# parse "00:00:00:00" to minutes
def parse_clock(t_str: str) -> int:
    parts = t_str.split(":")
    n = len(parts)
    D = H = M = S = 0
    if n == 4:
        D, H, M, S = map(int, parts)
    elif n == 3:
        H, M, S = map(int, parts)
    elif n == 2:
        M, S = map(int, parts)

    return D * 24 * 60 + H * 60 + M + round(S / 60)


#   "02/17 15:56:28"
def parse_datetime(t_str: str) -> datetime.datetime:
    return datetime.datetime.strptime(t_str, "%m/%d %H:%M:%S")


class LsfScheduler(SubprocessSchedulerInterface):
    status_exe = "bjobs"
    submit_exe = "bsub"
    delete_exe = "bkill"
    backfill_exe = "bslots"
    default_submit_kwargs: Dict[str, str] = {}
    submit_kwargs_flag_map: Dict[str, str] = {}

    _queue_name = "batch"

    # maps scheduler states to Balsam states
    _job_states = {
        "PEND": "queued",
        "RUN": "running",
        "BLOCKED": "failed",
    }

    @staticmethod
    def _job_state_map(scheduler_state: str) -> str:
        return LsfScheduler._job_states.get(scheduler_state, "unknown")

    # maps Balsam status fields to the scheduler fields
    # should be a comprehensive list of scheduler status fields
    _status_fields = {
        "scheduler_id": "JOBID",
        "state": "STAT",
        "queue": "QUEUE",
        "num_nodes": "NREQ_SLOT",
        "wall_time_min": "RUNTIMELIMIT",
        "project": "PROJ_NAME",
        "time_remaining_min": "RUN_TIME",
        "queued_time_min": "PEND_TIME",
    }

    @staticmethod
    def _get_envs() -> Dict[str, str]:
        env = {}
        fields = LsfScheduler._status_fields.values()
        env["LSB_BJOBS_FORMAT"] = " ".join(fields)
        return env

    # when reading these fields from the scheduler apply
    # these maps to the string extracted from the output
    @staticmethod
    def _status_field_map(balsam_field: str) -> Optional[Callable[[str], Any]]:
        status_field_map = {
            "scheduler_id": lambda id: int(id),
            "state": lambda state: LsfScheduler._job_states[state],
            "queue": lambda queue: str(queue),
            "num_nodes": lambda n: 0 if n == "-" else int(n),
            "wall_time_min": lambda minutes: int(float(minutes)),
            "project": lambda project: str(project),
            "time_remaining_min": lambda time: int(int(time.split()[0]) / 60),
            "queued_time_min": lambda minutes: int(minutes),
        }
        return status_field_map.get(balsam_field, None)

    @staticmethod
    def _render_submit_args(
        script_path: PathLike, project: str, queue: str, num_nodes: int, wall_time_min: int, **kwargs: Any
    ) -> List[str]:
        args = [
            LsfScheduler.submit_exe,
            "-o",
            os.path.basename(os.path.splitext(script_path)[0]) + ".output",
            "-e",
            os.path.basename(os.path.splitext(script_path)[0]) + ".error",
            "-P",
            project,
            "-q",
            queue,
            "-nnodes",
            str(int(num_nodes)),
            "-W",
            str(int(wall_time_min)),
        ]
        # adding additional flags as needed, e.g. `-C knl`
        for key, default_value in LsfScheduler.default_submit_kwargs.items():
            flag = LsfScheduler.submit_kwargs_flag_map[key]
            value = kwargs.setdefault(key, default_value)
            args += [flag, value]

        args.append(str(script_path))
        return args

    @staticmethod
    def _render_status_args(
        project: Optional[str] = None, user: Optional[str] = None, queue: Optional[str] = None
    ) -> List[str]:
        os.environ.update(LsfScheduler._get_envs())
        args = [LsfScheduler.status_exe]
        if user is not None:
            args += ["-u", user]
        if project is not None:
            pass  # not supported
        if queue is not None:
            args += ["-q", queue]
        # format output as json
        args += ["-json"]
        return args

    @staticmethod
    def _render_delete_args(job_id: Union[int, str]) -> List[str]:
        return [LsfScheduler.delete_exe, str(job_id)]

    @staticmethod
    def _render_backfill_args() -> List[str]:
        return [LsfScheduler.backfill_exe, '-R"select[CN]"']

    @classmethod
    def get_backfill_windows(cls) -> Dict[str, List[SchedulerBackfillWindow]]:
        backfill_args = cls._render_backfill_args()
        try:
            stdout = scheduler_subproc(backfill_args)
        except SchedulerNonZeroReturnCode as e:
            if "No backfill window meets" in str(e):
                return {LsfScheduler._queue_name: []}
            raise
        backfill_windows = cls._parse_backfill_output(stdout)
        return backfill_windows

    @staticmethod
    def _parse_submit_output(submit_output: str) -> int:
        try:
            start = len("Job <")
            end = submit_output.find(">", start)
            scheduler_id = int(submit_output[start:end])
        except ValueError:
            scheduler_id = int(submit_output.split()[-1])
        return scheduler_id

    @staticmethod
    def _parse_status_output(raw_output: str) -> Dict[int, SchedulerJobStatus]:
        # Example output:
        # {
        # "COMMAND":"bjobs",
        # "JOBS":47,
        # "RECORDS":[
        #   {
        #     "JOBID":"806290",
        #     "STAT":"RUN",
        #     "QUEUE":"batch",
        #     "PROJ_NAME":"BIP152",
        #     "PEND_TIME":"17",
        #     "NREQ_SLOT":"43",
        #     "RUNTIMELIMIT":"1440.0",
        #     "RUN_TIME":"16038 second(s)"
        #   },
        json_output = json.loads(raw_output)
        status_dict = {}
        batch_jobs = json_output["RECORDS"]
        for job_data in batch_jobs:
            status = {}
            try:
                for balsam_key, scheduler_key in LsfScheduler._status_fields.items():
                    func = LsfScheduler._status_field_map(balsam_key)
                    if callable(func):
                        status[balsam_key] = func(job_data[scheduler_key])
            except KeyError:
                logging.exception("failed parsing job data: %s", job_data)
            else:
                job_stat = SchedulerJobStatus(**status)
                status_dict[job_stat.scheduler_id] = job_stat

        return status_dict

    @staticmethod
    def _parse_backfill_output(stdout: str) -> Dict[str, List[SchedulerBackfillWindow]]:
        raw_lines = stdout.split("\n")
        windows: Dict[str, List[SchedulerBackfillWindow]] = {LsfScheduler._queue_name: []}
        node_lines = raw_lines[1:]
        for line in node_lines:
            if len(line.strip()) == 0:
                continue
            windows[LsfScheduler._queue_name].append(LsfScheduler._parse_bslots_line(line))
        return windows

    @staticmethod
    def _parse_bslots_line(line: str) -> SchedulerBackfillWindow:
        parts = line.split()
        nodes = int(parts[0])
        backfill_time = 0
        if len(re.findall("hours.*minutes.*seconds", line)) > 0:
            backfill_time += int(parts[1]) * 60
            backfill_time += int(parts[3])
        elif len(re.findall("minutes.*seconds", line)) > 0:
            backfill_time += int(parts[1])

        return SchedulerBackfillWindow(num_nodes=nodes, wall_time_min=backfill_time)

    @staticmethod
    def _parse_logs(scheduler_id: Union[int, str], job_script_path: Optional[PathLike]) -> SchedulerJobLog:
        # TODO: Return job start/stop time from log file or command
        return SchedulerJobLog()
