from getpass import getuser
import subprocess
import os

class SchedulerNonZeroReturnCode(Exception): pass

def scheduler_subproc(args):
    p = subprocess.run(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        encoding='utf-8',
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
    
    def submit(self, script_path, project, queue, num_nodes, time_minutes):
        """
        Submit the script at `script_path` to a local job queue.
        Returns scheduler ID of the submitted job.
        """
        raise NotImplementedError
    
    def get_statuses(self):
        """
        Returns list of JobStatus for each job belonging to current user
        """
        raise NotImplementedError

    def delete_job(self, scheduler_id):
        """
        Deletes the batch job matching `scheduler_id`
        """
        raise NotImplementedError

    def get_site_nodelist(self):
        """
        Returns a list of NodeWindows on the system
        """
        raise NotImplementedError

    def check_queues(self):
        """
        Returns list of JobStatus for all queued jobs on the system
        """
        raise NotImplementedError


class SubprocessSchedulerInterface(SchedulerInterface):

    def submit(self, script_path, project, queue, num_nodes, time_minutes):
        submit_args = self._render_submit_args(
            script_path, project, queue, num_nodes, time_minutes
        )
        stdout = scheduler_subproc(submit_args)
        scheduler_id = self._parse_submit_output(stdout)
        return scheduler_id

    def get_statuses(self):
        stat_args = self._render_status_args(user=self.username)
        stdout = scheduler_subproc(stat_args)
        stat_dict = self._parse_status_output(stdout)
        return stat_dict

    def delete_job(self, scheduler_id):
        delete_args = self._render_delete_args(scheduler_id)
        stdout = scheduler_subproc(delete_args)
        return stdout

    def get_site_nodelist(self):
        """
        Get available compute nodes system-wide
        """
        nodelist_args = self._render_nodelist_args()
        stdout = scheduler_subproc(nodelist_args)
        nodelist = self._parse_nodelist(stdout)
        return nodelist

    def check_queues(self):
        """
        Get all queues with lists of enqueued or running jobs
        """
        stat_args = self._render_status_args(user=None)
        stdout = scheduler_subproc(stat_args)
        stat_dict = self._parse_status_output(stdout)
        return stat_dict
