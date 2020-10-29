from .scheduler import (
    SubprocessSchedulerInterface,
    SchedulerJobStatus,
    SchedulerBackfillWindow,
)


def parse_time_minutes(t_str):
    try:
        return int(t_str)
    except ValueError:
        return 0


def parse_backfill_time(t_str):
    if t_str == "-":
        return 0

    parts = t_str.split(":")
    hr = int(parts[0])
    min = int(parts[1])
    # sec = int(parts[2])

    return hr * 60 + min


class DummyScheduler(SubprocessSchedulerInterface):
    status_exe = "echo"
    submit_exe = "bash"
    delete_exe = "echo"
    backfill_exe = "echo"

    # maps scheduler states to Balsam states
    # the keys of this dictionary should
    # also be a representative list of all possible states
    # that this scheduler recognizes
    job_states = {
        "QUEUED": "queued",
        "STARTING": "starting",
        "RUNNING": "running",
        "EXITING": "exiting",
        "USER_HOLD": "user_hold",
        "DEP_HOLD": "dep_hold",
        "DEP_FAIL": "dep_fail",
        "ADMIN_HOLD": "admin_hold",
        "FINISHED": "finished",
        "FAILED": "failed",
    }

    @staticmethod
    def _job_state_map(scheduler_state):
        return DummyScheduler.job_states.get(scheduler_state, "unknown")

    # maps Balsam status fields to the scheduler fields
    # should be a comprehensive list of scheduler status fields
    status_fields = {
        "id": "job_id",
        "state": "state",
        "wall_time_min": "wall_time",
        "queue": "queue_name",
        "num_nodes": "nodes",
        "project": "project",
        "time_remaining_min": "time_left",
    }

    # when reading these fields from the scheduler apply
    # these maps to the string extracted from the output
    @staticmethod
    def _status_field_map(balsam_field):
        status_field_map = {
            "id": lambda id: int(id),
            "num_nodes": lambda n: int(n),
            "time_remaining_min": parse_time_minutes,
            "wall_time_min": parse_time_minutes,
            "state": DummyScheduler._job_state_map,
        }
        return status_field_map.get(balsam_field, lambda x: x)

    # maps node list states to Balsam node states
    node_states = {
        "busy": "busy",
        "idle": "idle",
    }

    @staticmethod
    def _node_state_map(nodelist_state):
        return DummyScheduler.node_states.get(nodelist_state, "unknown")

    # maps the Balsam status fields to the node list fields
    # should be a comprehensive list of node list fields
    nodelist_fields = {
        "id": "Node_id",
        "name": "Name",
        "queues": "Queues",
        "state": "Status",
        "mem": "MCDRAM",
        "numa": "NUMA",
        "backfill_time": "Backfill",
    }

    # when reading these fields from the scheduler apply
    # these maps to the string extracted from the output
    @staticmethod
    def _nodelist_field_map(balsam_field):
        nodelist_field_map = {
            "id": lambda id: int(id),
            "queues": lambda q: q.split(":"),
            "state": DummyScheduler._node_state_map,
            "backfill_time": lambda x: parse_backfill_time(x),
        }
        return nodelist_field_map.get(balsam_field, lambda x: x)

    _nodelist_output = """Node_id  Name          Queues                                                                                                                        Status           MCDRAM  NUMA  Backfill
=================================================================================================================================================================================================
0        c0-0c0s0n0    debug-flat-quad                                                                                                               idle             flat    quad  -
1        c0-0c0s0n1    debug-flat-quad                                                                                                               idle             flat    quad  -
23       c0-0c0s5n3    debug-flat-quad:build                                                                                                         idle             flat    quad  -
24       c0-0c0s6n0    backfill-flat-quad:flat-quad:all-nodes:backfill-all-nodes:default:backfill:balsam:analysis                                    busy             cache   quad  1:22:58
25       c0-0c0s6n1    backfill-flat-quad:flat-quad:all-nodes:backfill-all-nodes:default:backfill:balsam:analysis                                    busy             cache   quad  1:22:58"""

    _status_output = """job_id state wall_time queue_name nodes project time_left
==============
 4   QUEUED  60  default  20  p1  NA
 2   USER_HOLD  60  default  20  p1  NA
 8   DEP_HOLD  60  default  20  p1  NA
 10   DEP_FAIL  60  default  20  p1  NA
 22   FAILED  60  default  20  p1  NA
 43   RUNNING  120  default 60  p2  40
 48   FINISHED  120  default 60  p2  40
 50   FINISHED  55 debug  2  p3  NA"""

    def _get_envs(self):
        env = {}
        # fields = self.status_fields.values()
        # env['QSTAT_HEADER'] = ':'.join(fields)
        return env

    def _render_submit_args(
        self, script_path, project, queue, num_nodes, wall_time_min
    ):
        args = [
            self.submit_exe,
            script_path,
            "--num_nodes",
            str(num_nodes),
            "--project",
            project,
            "--queue",
            queue,
            "--walltime",
            str(wall_time_min),
        ]
        return args

    @staticmethod
    def _parse_submit_output(stdout):
        for line in stdout.split("\n"):
            if "JOBID=" in line:
                return int(line.split("JOBID=")[1])
        return None

    def _render_status_args(self, project=None, user=None, queue=None):
        args = [self.status_exe]
        if user is not None:
            args += ["-u", user]
        if project is not None:
            args += ["-A", project]
        if queue is not None:
            args += ["-q", queue]
        return args

    def _parse_status_output(self, stdout):
        stdout = self._status_output
        raw_lines = stdout.split("\n")
        # header = raw_lines[0].split()
        status_dict = {}
        job_lines = raw_lines[2:]
        for line in job_lines:
            job_stat = self._parse_status_line(line)
            if job_stat:
                id = int(job_stat.scheduler_id)
                status_dict[id] = job_stat
        return status_dict

    def _parse_status_line(self, line):
        fields = line.split()
        if len(fields) != len(self.status_fields):
            return SchedulerJobStatus()

        status = {}
        for name, value in zip(self.status_fields, fields):
            func = DummyScheduler._status_field_map(name)
            status[name] = func(value)

        return SchedulerJobStatus(**status)

    def _render_delete_args(self, job_id):
        args = [
            self.delete_exe,
            str(job_id),
        ]

        return args

    def _render_backfill_args(self):
        args = [
            self.backfill_exe,
            self._nodelist_output,
        ]
        return args

    def _parse_backfill_output(self, stdout):
        # parse stdout here

        # build example output dictionary
        windows = {
            "default_queue": [
                SchedulerBackfillWindow(num_nodes=5, wall_time_min=60),
                SchedulerBackfillWindow(num_nodes=15, wall_time_min=45),
            ],
            "debug_queue": [
                SchedulerBackfillWindow(num_nodes=1, wall_time_min=60),
                SchedulerBackfillWindow(num_nodes=3, wall_time_min=20),
            ],
        }
        return windows
