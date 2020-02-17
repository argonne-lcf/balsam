from .scheduler import SubprocessSchedulerInterface
from .scheduler import JobStatus
from .scheduler import BackfillWindow
import os
import re
import logging

logger = logging.getLogger(__name__)


# parse "00:00:00" to minutes
def parse_clock(t_str):
    parts = t_str.split(":")
    n = len(parts)
    H = M = S = 0
    if n == 3:
        H, M, S = map(int, parts)
    elif n == 2:
        M, S = map(int, parts)

    return H * 60 + M + round(S / 60)


# parse "1-00:00:00" to minutes
def parse_time_minutes(t_str):
    t_str.replace("L", "")
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


class LsfScheduler(SubprocessSchedulerInterface):
    status_exe = "bjobs"
    submit_exe = "bsub"
    delete_exe = "bkill"
    backfill_exe = "bslots"
    default_submit_kwargs = {}
    submit_kwargs_flag_map = {}

    # maps scheduler states to Balsam states
    job_states = {
        "PEND": "queued",
        "RUN": "running",
        "DONE": "finished",
        "EXIT": "failed",
        "PSUSP": "cancelled",
        "USUSP": "cancelled",
        "SSUSP": "cancelled",
    }

    @staticmethod
    def _job_state_map(scheduler_state):
        return LsfScheduler.job_states.get(scheduler_state, "unknown")

    # maps Balsam status fields to the scheduler fields
    # should be a comprehensive list of scheduler status fields
    status_fields = {
        "id": "jobid",
        "state": "stat",
        "nodes": "slots",
        "queue": "queue",
        "wall_time_min": "runtimelimit",
        "project": "proj_name",
        "time_remaining_min": "time_left",
    }

    # when reading these fields from the scheduler apply
    # these maps to the string extracted from the output
    @staticmethod
    def _status_field_map(balsam_field):
        status_field_map = {
            "id": lambda id: int(id),
            "state": LsfScheduler._job_state_map,
            "wall_time_min": lambda x: int(float(x)),
            "nodes": lambda n: 0 if n == "-" else int(n),
            "time_remaining_min": parse_time_minutes,
        }
        return status_field_map.get(balsam_field, lambda x: x)

    # maps node list states to Balsam node states
    node_states = {
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
    def _node_state_map(nodelist_state):
        try:
            return LsfScheduler.node_states[nodelist_state]
        except KeyError:
            logger.warning("node state %s is not recognized", nodelist_state)
            return "unknown"

    def _get_envs(self):
        env = {}
        fields = self.status_fields.values()
        env["LSB_BJOBS_FORMAT"] = " ".join(fields)
        return env

    def _render_submit_args(
        self, script_path, project, queue, num_nodes, time_minutes, **kwargs
    ):
        args = [
            self.submit_exe,
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
            str(int(time_minutes)),
        ]
        # adding additional flags as needed, e.g. `-C knl`
        for key, default_value in self.default_submit_kwargs.items():
            flag = self.submit_kwargs_flag_map[key]
            value = kwargs.setdefault(key, default_value)
            args += [flag, value]

        args.append(script_path)
        return args

    def _render_status_args(self, project=None, user=None, queue=None):
        args = [self.status_exe]
        if user is not None:
            args += ["-u", user]
        if project is not None:
            args += ["-P", project]
        if queue is not None:
            pass  # not supported on LSF
        return args

    def _render_delete_args(self, job_id):
        return [self.delete_exe, str(job_id)]

    def _render_backfill_args(self):
        return [self.backfill_exe, '-R"select[CN]"']

    def _parse_submit_output(self, submit_output):
        try:
            start = len("Job <")
            end = submit_output.find(">", start)
            scheduler_id = int(submit_output[start:end])
        except ValueError:
            scheduler_id = int(submit_output.split()[-1])
        return scheduler_id

    def _parse_status_output(self, raw_output):
        status_dict = {}
        job_lines = raw_output.strip().split("\n")[1:]
        for line in job_lines:
            if len(line.strip()) == 0:
                continue
            job_stat = self._parse_status_line(line)
            if job_stat:
                status_dict[job_stat.id] = job_stat
        return status_dict

    def _parse_status_line(self, line):
        fields = line.split()
        if len(fields) - len(self.status_fields) > 1:
            return JobStatus()

        status = {}
        for name, value in zip(self.status_fields, fields):
            func = self._status_field_map(name)
            status[name] = func(value)
        return JobStatus(**status)

    def _parse_backfill_output(self, stdout):
        raw_lines = stdout.split("\n")
        windows = {"batch": []}
        node_lines = raw_lines[1:]
        for line in node_lines:
            if len(line.strip()) == 0:
                continue
            windows["batch"].append(self._parse_nodelist_line(line))
        return windows

    def _parse_nodelist_line(self, line):
        parts = line.split()
        nodes = int(parts[0])
        backfill_time = 0
        if len(re.findall("hours.*minutes.*seconds", line)) > 0:
            backfill_time += int(parts[1]) * 60
            backfill_time += int(parts[3])
        elif len(re.findall("minutes.*seconds", line)) > 0:
            backfill_time += int(parts[1])

        return BackfillWindow(num_nodes=nodes, backfill_time_min=backfill_time)
