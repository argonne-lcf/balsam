import datetime
import logging
import os
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

from dateutil.parser import ParserError

from balsam.util import parse_to_utc

from .scheduler import (
    SchedulerBackfillWindow,
    SchedulerJobLog,
    SchedulerJobStatus,
    SubprocessSchedulerInterface,
    scheduler_subproc,
)

PathLike = Union[Path, str]
logger = logging.getLogger(__name__)


# parse "00:00:00" to minutes
def parse_clock(t_str: str) -> int:
    parts = t_str.split(":")
    n = len(parts)
    H = M = S = 0
    if n == 3:
        H, M, S = map(int, parts)
    elif n == 2:
        M, S = map(int, parts)

    return H * 60 + M + round(S / 60)


# parse "1-00:00:00" to minutes
def parse_time_minutes(t_str: str) -> Optional[int]:
    mins = 0
    try:
        parts = t_str.split("-")
        if len(parts) == 1:
            mins += parse_clock(parts[0])
        elif len(parts) == 2:
            mins += parse_clock(parts[1])
            mins += int(parts[0]) * 24 * 60
    except ValueError:
        return None
    else:
        return mins


# parse "2021-02-20T13:11:53" to minutes
def parse_queued_time(t_str: str) -> Optional[int]:
    mins = 0.0
    try:
        submit_time = datetime.datetime.strptime(t_str, "%Y-%m-%dT%H:%M:%S")
        timediff = datetime.datetime.now() - submit_time
        mins = timediff.total_seconds() / 60.0
    except Exception:
        logger.exception(f"exception while processing {t_str}")
        return None
    else:
        return int(mins)


class SlurmScheduler(SubprocessSchedulerInterface):
    status_exe = "squeue"
    submit_exe = "sbatch"
    delete_exe = "scancel"
    backfill_exe = "sinfo"
    history_exe = "sacct"
    default_submit_kwargs: Dict[str, str] = {}
    submit_kwargs_flag_map: Dict[str, str] = {}

    # maps scheduler states to Balsam states
    job_states = {
        "PENDING": "queued",
        "CONFIGURING": "running",
        "RUNNING": "running",
        "COMPLETING": "running",
        "RESV_DEL_HOLD": "queued",
        "ADMIN_HOLD": "queued",
        "FINISHED": "finished",
        "FAILED": "failed",
        "CANCELLED": "finished",
        "DEADLINE": "finished",
        "PREEMPTED": "finished",
        "REQUEUED": "queued",
        "SIGNALING": "running",
        "STAGE_OUT": "running",
        "TIMEOUT": "failed",
    }

    @staticmethod
    def _job_state_map(scheduler_state: str) -> str:
        return SlurmScheduler.job_states.get(scheduler_state, "unknown")

    # maps Balsam status fields to the scheduler fields
    # should be a comprehensive list of scheduler status fields
    _status_fields = {
        "scheduler_id": "jobid",
        "state": "state",
        "queue": "partition",
        "num_nodes": "numnodes",
        "wall_time_min": "timelimit",
        "project": "account",
        "time_remaining_min": "timeleft",
        "queued_time_min": "submittime",
    }

    # when reading these fields from the scheduler apply
    # these maps to the string extracted from the output
    @staticmethod
    def _status_field_map(balsam_field: str) -> Optional[Callable[[str], Any]]:
        status_field_map = {
            "scheduler_id": lambda id: int(id),
            "state": SlurmScheduler._job_state_map,
            "queue": lambda queue: str(queue),
            "num_nodes": lambda n: int(n),
            "wall_time_min": parse_time_minutes,
            "project": lambda project: str(project),
            "time_remaining_min": parse_time_minutes,
            "queued_time_min": parse_queued_time,
        }
        return status_field_map.get(balsam_field, None)

    # maps node list states to Balsam node states
    # descriptions: https://slurm.schedmd.com/sinfo.html
    _node_states = {
        "alloc": "busy",  # allocated
        "boot": "busy",
        "comp": "busy",  # completing
        "down": "busy",
        "drain": "busy",  # drained
        "drng": "busy",  # draining
        "fail": "busy",
        "failg": "busy",  # failing
        "futr": "busy",  # future
        "idle": "idle",
        "maint": "busy",  # maintenance
        "mix": "busy",
        "npc": "busy",  # perfctrs
        "pow_dn": "busy",  # power down
        "pow_up": "busy",  # power up
        "resv": "busy",  # reserved
        "unk": "busy",  # unknown
    }

    @staticmethod
    def _node_state_map(nodelist_state: str) -> str:
        try:
            # removing special symbols that have some meaning
            # in the future we might want to encode this info
            # * The node is presently not responding and will not be allocated any new work.
            nodelist_state = nodelist_state.replace("*", "")
            # ~ The node is presently in a power saving mode
            nodelist_state = nodelist_state.replace("~", "")
            # # The node is presently being powered up or configured.
            nodelist_state = nodelist_state.replace("#", "")
            # % The node is presently being powered down.
            nodelist_state = nodelist_state.replace("%", "")
            # $ The node is currently in a reservation with a flag value of "maintenance".
            nodelist_state = nodelist_state.replace("$", "")
            # @ The node is pending reboot.
            nodelist_state = nodelist_state.replace("@", "")
            # alloc+ The node is allocated to one or more active jobs plus one or more jobs are in the process of COMPLETING.
            nodelist_state = nodelist_state.replace("+", "")
            return SlurmScheduler._node_states[nodelist_state]
        except KeyError:
            logger.warning("node state %s is not recognized", nodelist_state)
            return "unknown"

    # maps the Balsam status fields to the node list fields
    # should be a comprehensive list of node list fields
    _nodelist_fields = {
        "id": "nodelist",
        "queues": "partition",
        "node_state": "stateshort",
    }

    _fields_encondings = {
        "nodelist": "%N",
        "partition": "%P",
        "stateshort": "%t",
    }

    # when reading these fields from the scheduler apply
    # these maps to the string extracted from the output
    @staticmethod
    def _backfill_field_map(balsam_field: str) -> Callable[[str], Any]:
        nodelist_field_map = {
            "queues": lambda q: q.split(":"),
            "state": SlurmScheduler._node_state_map,
            "backfill_time": parse_time_minutes,
        }
        return nodelist_field_map.get(balsam_field, lambda x: x)

    @staticmethod
    def _set_envs() -> Dict[str, str]:
        env = {}
        fields = SlurmScheduler._status_fields.values()
        env["SQUEUE_FORMAT2"] = ",".join(fields)
        fields = SlurmScheduler._nodelist_fields.values()
        env["SINFO_FORMAT"] = " ".join(SlurmScheduler._fields_encondings[field] for field in fields)
        os.environ.update(env)
        return env

    @staticmethod
    def _render_submit_args(
        script_path: PathLike, project: str, queue: str, num_nodes: int, wall_time_min: int, **kwargs: Any
    ) -> List[str]:
        args = [
            SlurmScheduler.submit_exe,
            "-o",
            os.path.basename(os.path.splitext(script_path)[0]) + ".output",
            "-e",
            os.path.basename(os.path.splitext(script_path)[0]) + ".error",
            "-A",
            project,
            "-q",
            queue,
            "-N",
            str(int(num_nodes)),
            "-t",
            str(int(wall_time_min)),
        ]
        # adding additional flags as needed, e.g. `-C knl`
        for key, default_value in SlurmScheduler.default_submit_kwargs.items():
            flag = SlurmScheduler.submit_kwargs_flag_map[key]
            value = kwargs.setdefault(key, default_value)
            args += [flag, value]

        args.append(str(script_path))
        return args

    @staticmethod
    def _render_status_args(
        project: Optional[str] = None, user: Optional[str] = None, queue: Optional[str] = None
    ) -> List[str]:
        SlurmScheduler._set_envs()
        args = [SlurmScheduler.status_exe]
        if user is not None:
            args += ["-u", user]
        if project is not None:
            args += ["-A", project]
        if queue is not None:
            args += ["-q", queue]
        return args

    @staticmethod
    def _render_delete_args(job_id: Union[int, str]) -> List[str]:
        return [SlurmScheduler.delete_exe, str(job_id)]

    @staticmethod
    def _render_backfill_args() -> List[str]:
        SlurmScheduler._set_envs()
        return [SlurmScheduler.backfill_exe]

    @staticmethod
    def _parse_submit_output(submit_output: str) -> int:
        try:
            scheduler_id = int(submit_output)
        except ValueError:
            scheduler_id = int(submit_output.split()[-1])
        return scheduler_id

    @staticmethod
    def _parse_status_output(raw_output: str) -> Dict[int, SchedulerJobStatus]:
        # TODO: this can be much more efficient with a compiled regex findall()
        status_dict = {}
        job_lines = raw_output.strip().split("\n")[1:]
        for line in job_lines:
            try:
                job_stat = SlurmScheduler._parse_status_line(line)
            except (ValueError, TypeError):
                logger.exception(f"Cannot parse job status: {line}")
                continue
            else:
                status_dict[job_stat.scheduler_id] = job_stat
        return status_dict

    @staticmethod
    def _parse_status_line(line: str) -> SchedulerJobStatus:
        fields = line.split()
        actual = len(fields)
        expected = len(SlurmScheduler._status_fields)
        if actual != expected:
            logger.warning(f"Line has {actual} columns: expected {expected}:\n{fields}")
            raise ValueError(f"Line has {actual} columns: expected {expected}:\n{fields}")
        logger.debug(f"parsing fields: {fields}")
        status = {}
        for name, value in zip(SlurmScheduler._status_fields, fields):
            func = SlurmScheduler._status_field_map(name)
            if callable(func):
                status[name] = func(value)

        return SchedulerJobStatus(**status)

    @staticmethod
    def _parse_backfill_output(stdout: str) -> Dict[str, List[SchedulerBackfillWindow]]:
        # TODO: NERSC currently does not provide this kind of information
        return {}

    @staticmethod
    def _parse_logs(scheduler_id: Union[int, str], job_script_path: Optional[PathLike]) -> SchedulerJobLog:
        # use command to get batch job runtime
        args = [SlurmScheduler.history_exe]
        args += ["-o", "JobId,Start,End"]  # output format
        args += ["-j", str(scheduler_id)]  # for this job id
        args += ["-n"]  # no header
        stdout = scheduler_subproc(args)
        # ---- output like this:
        # 40589507     2021-03-12T08:40:12 2021-03-12T08:41:18
        # 40589507.ba+ 2021-03-12T08:40:12 2021-03-12T08:41:18
        # 40589507.ex+ 2021-03-12T08:40:12 2021-03-12T08:41:21
        # 40589507.0   2021-03-12T08:40:42 2021-03-12T08:40:45
        # returns empty string with newline for not found jobs:
        if len(stdout) <= 5:
            return SchedulerJobLog()
        first_line = stdout.strip().split("\n")[0]  # only take first line
        job_data = first_line.split()
        try:
            start_time = parse_to_utc(job_data[1])
            end_time = parse_to_utc(job_data[2])
        except ParserError:
            logger.warning(f"Failed to parse job_data times (START_TIME: {job_data[1]}) (FINISH_TIME: {job_data[2]})")
            return SchedulerJobLog()
        return SchedulerJobLog(start_time=start_time, end_time=end_time)
