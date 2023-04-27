import getpass
import json
import logging
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

import dateutil.parser

from balsam.util import parse_to_utc

from .scheduler import (
    DelayedSubmitFail,
    SchedulerBackfillWindow,
    SchedulerJobLog,
    SchedulerJobStatus,
    SchedulerNonZeroReturnCode,
    SubprocessSchedulerInterface,
    scheduler_subproc,
)

PathLike = Union[Path, str]

logger = logging.getLogger(__name__)


def parse_cobalt_time_minutes(t_str: str) -> int:
    try:
        H, M, S = map(int, t_str.split(":"))
    except ValueError:
        return 0
    else:
        return H * 60 + M + round(S / 60)


class PBSScheduler(SubprocessSchedulerInterface):
    status_exe = "qstat"
    submit_exe = "qsub"
    delete_exe = "qdel"
    backfill_exe = "pbsnodes"

    # maps scheduler states to Balsam states
    _job_states = {
        "Q": "queued",
        "H": "queued",
        "T": "queued",
        "W": "queued",
        "S": "queued",
        "R": "running",
        "E": "running",
    }

    @staticmethod
    def _job_state_map(scheduler_state: str) -> str:
        return PBSScheduler._job_states.get(scheduler_state, "unknown")

    # maps Balsam status fields to the scheduler fields
    # should be a comprehensive list of scheduler status fields
    _status_fields = {
        "scheduler_id": "JobID",
        "state": "State",
        "wall_time_min": "WallTime",
        "queue": "Queue",
        "num_nodes": "Nodes",
        "project": "Project",
        "time_remaining_min": "TimeRemaining",
        "queued_time_min": "QueuedTime",
    }

    # when reading these fields from the scheduler apply
    # these maps to the string extracted from the output
    @staticmethod
    def _status_field_map(balsam_field: str) -> Optional[Callable[[str], Any]]:
        status_field_map: Dict[str, Callable[[str], Any]] = {
            "scheduler_id": lambda id: int(id),
            "state": PBSScheduler._job_state_map,
            "queue": lambda queue: str(queue),
            "num_nodes": lambda n: int(n),
            "time_remaining_min": parse_cobalt_time_minutes,
            "queued_time_min": parse_cobalt_time_minutes,
            "project": lambda project: str(project),
            "wall_time_min": parse_cobalt_time_minutes,
        }
        return status_field_map.get(balsam_field, None)

    # maps node list states to Balsam node states
    _node_states = {
        "busy": "busy",
        "idle": "idle",
        "cleanup-pending": "busy",
        "down": "busy",
        "allocated": "busy",
    }

    @staticmethod
    def _node_state_map(nodelist_state: str) -> str:
        try:
            return PBSScheduler._node_states[nodelist_state]
        except KeyError:
            logger.warning("node state %s is not recognized", nodelist_state)
            return "unknown"

    # maps the Balsam status fields to the node list fields
    # should be a comprehensive list of node list fields
    _nodelist_fields = {
        "id": "Node_id",
        "name": "Name",
        "queues": "Queues",
        "state": "Status",
        "mem": "MCDRAM",
        "numa": "NUMA",
        "wall_time_min": "Backfill",
    }

    # when reading these fields from the scheduler apply
    # these maps to the string extracted from the output
    @staticmethod
    def _nodelist_field_map(balsam_field: str) -> Callable[[str], Any]:
        nodelist_field_map = {
            "id": lambda id: int(id),
            "state": PBSScheduler._node_state_map,
            "queues": lambda x: x.split(":"),
            "wall_time_min": lambda x: parse_cobalt_time_minutes(x),
        }
        return nodelist_field_map.get(balsam_field, lambda x: x)

    @staticmethod
    def _render_submit_args(
        script_path: Union[Path, str], project: str, queue: str, num_nodes: int, wall_time_min: int, **kwargs: Any
    ) -> List[str]:
        hours = wall_time_min // 60
        minutes = wall_time_min - hours * 60
        args = [
            PBSScheduler.submit_exe,
            "-A",
            project,
            "-q",
            queue,
            "-l",
            f"select={num_nodes}",
            "-l",
            f"walltime={hours}:{minutes}:00",
            "-k",
            "doe",
            str(script_path),
        ]
        return args

    @staticmethod
    def _render_status_args(project: Optional[str], user: Optional[str], queue: Optional[str]) -> List[str]:
        pass

    @staticmethod
    def _render_delete_args(job_id: Union[int, str]) -> List[str]:
        return [PBSScheduler.delete_exe, str(job_id)]

    @staticmethod
    def _render_backfill_args() -> List[str]:
        return [PBSScheduler.backfill_exe, "-a", "-F", "json"]

    @staticmethod
    def _parse_submit_output(submit_output: str) -> int:
        try:
            return int(submit_output.split(".")[0])
        except Exception as exc:
            # Catch errors here and handle
            logger.warning(f"Exception: {exc}")
            raise

    @classmethod
    def get_statuses(
        cls,
        project: Optional[str] = None,
        user: Optional[str] = getpass.getuser(),
        queue: Optional[str] = None,
    ) -> Dict[int, SchedulerJobStatus]:
        # First call qstat to get user job ids
        args = [PBSScheduler.status_exe]
        stdout = scheduler_subproc(args)
        stdout_lines = [s for s in stdout.split("\n") if str(user) in s]
        if len(stdout_lines) == 0:
            return {}  # if there are no jobs in the queue return an empty dictionary
        user_job_ids = [s.split(".")[0] for s in stdout_lines]

        # Next call qstat to get job jsons
        args = [PBSScheduler.status_exe]
        args += user_job_ids
        args += "-f -F json".split()
        stdout = scheduler_subproc(args)
        stat_dict = cls._parse_status_output(stdout)
        return stat_dict

    @staticmethod
    def _parse_status_output(raw_output: str) -> Dict[int, SchedulerJobStatus]:
        # TODO: this can be much more efficient with a compiled regex findall()
        # logger.info(f"json status output {raw_output}")
        username = getpass.getuser()
        j = json.loads(raw_output)
        date_format = "%a %b %d %H:%M:%S %Y"
        status_dict = {}
        if "Jobs" in j.keys():
            try:
                for jobidstr, job in j["Jobs"].items():
                    # temporarily filter jobs by user due to PBS bug
                    job_username = job["Job_Owner"].split("@")[0]
                    if job_username != username:
                        continue
                    status = {}
                    try:
                        # array jobs can have a trailing "[]"; remove this
                        jobidstr = jobidstr.replace("[]", "")
                        jobid = int(jobidstr.split(".")[0])
                        status["scheduler_id"] = jobid
                    except ValueError:
                        logger.error(f"Error parsing jobid {jobidstr} in status output; skipping")
                        continue
                    status["state"] = PBSScheduler._job_states[job["job_state"]]  # type: ignore # noqa
                    status["time_remaining_min"] = 0
                    status["wall_time_min"] = 0
                    if "walltime" in job["Resource_List"].keys():
                        W = job["Resource_List"]["walltime"].split(":")
                        wall_time_min = int(W[0]) * 60 + int(W[1])  # 00:00:00
                        status["wall_time_min"] = wall_time_min
                        if status["state"] == "queued":  # type: ignore # noqa
                            status["time_remaining_min"] = wall_time_min
                        try:
                            if status["state"] == "running" and "stime" in job.keys():  # type: ignore # noqa
                                status["time_remaining_min"] = int(
                                    wall_time_min
                                    - (datetime.now() - datetime.strptime(job["stime"], date_format)).total_seconds()
                                    / 60
                                )
                        except Exception as err:
                            status["time_remaining_min"] = wall_time_min
                            logger.exception(f"Exception {str(err)} processing job {jobidstr} {job}")
                    status["queue"] = job["queue"]
                    status["num_nodes"] = job["Resource_List"]["nodect"]
                    status["project"] = job["project"]
                    status["queued_time_min"] = int(
                        (datetime.now() - datetime.strptime(job["qtime"], date_format)).total_seconds() / 60
                    )
                    status_dict[jobid] = SchedulerJobStatus(**status)
            except BaseException as err:
                logger.exception(f"Exception {str(err)} parsing {raw_output}")
        return status_dict

    @staticmethod
    def _parse_backfill_output(stdout: str) -> Dict[str, List[SchedulerBackfillWindow]]:
        # fill in this method later to support backfill
        # turam: input here will be json via "pbsnodes -a -F json"
        return dict()
        # prior cobalt impl follows
        raw_lines = stdout.strip().split("\n")
        nodelist = []
        node_lines = raw_lines[2:]
        logger.debug(node_lines)
        for line in raw_lines:
            try:
                line_dict = PBSScheduler._parse_nodelist_line(line)
            except (ValueError, TypeError):
                logger.debug(f"Cannot parse nodelist line: {line}")
            else:
                if line_dict["wall_time_min"] > 0 and line_dict["state"] == "idle":
                    nodelist.append(line_dict)

        windows = PBSScheduler._nodelist_to_backfill(nodelist)
        return windows

    @staticmethod
    def _parse_nodelist_line(line: str) -> Dict[str, Any]:
        fields = line.split()
        actual = len(fields)
        expected = len(PBSScheduler._nodelist_fields)

        if actual != expected:
            raise ValueError(f"Line has {actual} columns: expected {expected}:\n{fields}")

        status = {}
        for name, value in zip(PBSScheduler._nodelist_fields, fields):
            func = PBSScheduler._nodelist_field_map(name)
            status[name] = func(value)
        return status

    @staticmethod
    def _nodelist_to_backfill(
        nodelist: List[Dict[str, Any]],
    ) -> Dict[str, List[SchedulerBackfillWindow]]:
        queue_bf_times = defaultdict(list)
        windows = defaultdict(list)

        for entry in nodelist:
            bf_time = entry["wall_time_min"]
            queues = entry["queues"]
            for queue in queues:
                queue_bf_times[queue].append(bf_time)

        for queue, bf_times in queue_bf_times.items():
            # Mapping {bf_time: num_nodes}
            bf_counter = Counter(bf_times)
            # {
            #    queue_name: [(bf_time1, num_nodes1), (bf_time2, num_nodes2), ...],
            # }
            # For each queue, sorted with longer times first
            queue_bf_times[queue] = sorted(bf_counter.items(), reverse=True)

        for queue, bf_list in queue_bf_times.items():
            running_total = 0
            for bf_time, num_nodes in bf_list:
                running_total += num_nodes
                windows[queue].append(SchedulerBackfillWindow(num_nodes=running_total, wall_time_min=bf_time))
        return windows

    @staticmethod
    def _parse_time(line: str) -> datetime:
        time_str = line[: line.find("(UTC)")]
        return dateutil.parser.parse(time_str)

    @staticmethod
    def _parse_logs(scheduler_id: int, job_script_path: Optional[PathLike]) -> SchedulerJobLog:
        args = [PBSScheduler.status_exe]
        args += ["-x", "-f", "-F", "json"]
        args += [str(scheduler_id)]
        logger.info(f"_parse_logs issuing qstat: {str(args)}")
        try:
            stdout = scheduler_subproc(args)
        except SchedulerNonZeroReturnCode as e:
            if "Unknown Job Id" in str(e):
                logger.warning(f"Batch Job {scheduler_id} not found in PBS")
                raise DelayedSubmitFail
            return SchedulerJobLog()
        json_output = json.loads(stdout)
        # logger.info(f"_parse_logs json_output: {json_output}")
        if len(json_output["Jobs"]) == 0:
            logger.error("no job found for JOB ID = %s", scheduler_id)
            return SchedulerJobLog()
        job_data = list(json_output["Jobs"].values())[0]
        start_raw = job_data.get("stime")
        end_raw = job_data.get("mtime")
        if not (start_raw and end_raw):
            logger.warning(f"parse_logs got START_TIME: {start_raw}; FINISH_TIME: {end_raw}")
            return SchedulerJobLog()
        try:
            start = parse_to_utc(start_raw, local_zone="ET")
            end = parse_to_utc(end_raw, local_zone="ET")
        except dateutil.parser.ParserError:
            logger.warning(f"Failed to parse job_data times (START_TIME: {start_raw}) (FINISH_TIME: {end_raw})")
            return SchedulerJobLog()
        return SchedulerJobLog(start_time=start, end_time=end)

    @classmethod
    def discover_projects(cls) -> List[str]:
        """
        Get the user's allowed/preferred allocations
        Note: Could use sbank; currently uses Cobalt reporting of valid
              projects when an invalid project is given
        """
        """        click.echo("Checking with sbank for your current allocations...")
        with tempfile.NamedTemporaryFile() as fp:
            os.chmod(fp.name, 0o777)
            try:
                proc = subprocess.run(
                    "sbank projects -r polaris -f project_name --no-header --no-totals --no-sys-msg",
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    encoding="utf-8",
                )
                print(f"proc is {proc}")
                sbank_out = proc.stdout
                projects = [p.strip() for p in sbank_out.split("\n") if p]
            except:
                projects = None

        """
        projects = None
        if not projects:
            projects = super().discover_projects()
        return projects


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
