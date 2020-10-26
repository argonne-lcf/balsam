from getpass import getuser
import subprocess
import os
import collections


class SchedulerNonZeroReturnCode(Exception):
    pass


class SchedulerSubmitError(Exception):
    pass


""" JobStatus contains the status of a pending or running job """
JobStatus = collections.namedtuple(
    "JobStatus",
    [
        "id",
        "state",
        "queue",
        "num_nodes",
        "wall_time_min",
        "project",
        "time_remaining_min",
    ],
)


""" BackfillWindow contains a number of nodes which are free for some available time """
BackfillWindow = collections.namedtuple(
    "BackfillWindow", ["num_nodes", "backfill_time_min"]
)


def scheduler_subproc(args, cwd=None):
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
    def username(self):
        if self._username is None:
            self._username = getuser()
        return self._username

    def submit(
        self, script_path, project, queue, num_nodes, time_minutes, cwd=None, **kwargs
    ):
        """
        Submit the script at `script_path` to a local job queue.
        Returns scheduler ID of the submitted job.
        """
        raise NotImplementedError

    def get_statuses(self, project=None, user=None, queue=None):
        """
        Returns dictionary keyed on scheduler job id and a value of JobStatus for each
          job belonging to current user, project, and/or queue
        """
        raise NotImplementedError

    def delete_job(self, scheduler_id):
        """
        Deletes the batch job matching `scheduler_id`
        """
        raise NotImplementedError

    def get_backfill_windows(self):
        """
        Returns a dictionary keyed on queue name and a value of list of
          BackfillWindow on the system for available scheduling windows
        """
        raise NotImplementedError


class SubprocessSchedulerInterface(SchedulerInterface):
    def submit(
        self, script_path, project, queue, num_nodes, time_minutes, cwd=None, **kwargs
    ):
        submit_args = self._render_submit_args(
            script_path, project, queue, num_nodes, time_minutes, **kwargs
        )
        stdout = scheduler_subproc(submit_args, cwd)
        scheduler_id = self._parse_submit_output(stdout)
        return scheduler_id

    def get_statuses(self, project=None, user=None, queue=None):
        stat_args = self._render_status_args(project, user, queue)
        stdout = scheduler_subproc(stat_args)
        stat_dict = self._parse_status_output(stdout)
        return stat_dict

    def delete_job(self, scheduler_id):
        delete_args = self._render_delete_args(scheduler_id)
        stdout = scheduler_subproc(delete_args)
        return stdout

    def get_backfill_windows(self):
        backfill_args = self._render_backfill_args()
        stdout = scheduler_subproc(backfill_args)
        backfill_windows = self._parse_backfill_output(stdout)
        return backfill_windows
