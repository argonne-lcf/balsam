from balsam.site import conf
from .scheduler import SubprocessSchedulerInterface

def parse_cobalt_time_minutes(t_str):
    try:
        H, M, S = map(int, t_str.split(':'))
    except:
        return None
    else:
        return H*60 + M + round(S/60)

def state_map(s):
    states = {
        'queued': 'queued',
        'starting': 'starting',
        'running': 'running',
        'exiting': 'exiting',
        'user_hold': 'user_hold',
        'dep_hold': 'dep_hold',
        'dep_fail': 'dep_fail',
        'admin_hold': 'admin_hold',
    }
    return states.get(s, 'unknown')


class CobaltScheduler(SubprocessSchedulerInterface):
    status_exe = 'qstat'
    submit_exe = 'qsub'
    delete_exe = 'qdel'
    
    status_fields = {
        'id' : 'JobID',
        'time_remaining_min' : 'TimeRemaining',
        'wall_time_min' : 'WallTime',
        'state' : 'State',
        'queue' : 'Queue',
        'nodes' : 'Nodes',
        'project' : 'Project',
        'command' : 'Command',
        'user': 'User',
        'score': 'Score',
        'job_name' : 'JobName',
    }
    field_maps = {
        'id' : lambda id: int(id),
        'nodes' : lambda n: int(n)
        'time_remaining_min' : parse_cobalt_time_minutes,
        'wall_time_min' : parse_cobalt_time_minutes,
        'state' : state_map,
    }

    def _get_envs(self):
        env = {}
        fields = self.status_fields.values()
        env['QSTAT_HEADER'] = ':'.join(fields)
        return env

    def _render_submit_args(self, script_path, project, queue, num_nodes, time_minutes):
        args = [
            submit_exe,
            '--cwd', site.job_path,
            '-O', Path(script_path).stem,
            '-A', project,
            '-q', queue,
            '-n', str(int(num_nodes)),
            '-t', str(int(time_minutes)),
            script_path,
        ]
        return args
    
    def _render_status_args(self, user=None):
        args = [self.status_exe]
        if user is not None:
            args += ['-u', user]
        return args

    def _render_delete_args(self, job_id):
        return [self.delete_exe, str(job_id)]

    def _render_nodelist_args(self):
        pass
    
    def _parse_submit_output(self, submit_output):
        try: scheduler_id = int(submit_output)
        except ValueError: scheduler_id = int(submit_output.split()[-1])
        return scheduler_id

    def _parse_status_output(self, raw_output):
        # TODO: this can be much more efficient with a compiled regex findall()
        status_dict = {}
        job_lines = raw_output.split('\n')[2:]
        for line in job_lines:
            job_stat = self._parse_job_line(line)
            if job_stat:
                id = int(job_stat['id'])
                status_dict[id] = job_stat
        return status_dict

    def _parse_job_line(self, line):
        status = {}
        fields = line.split()
        if len(fields) != len(self.status_fields):
            return status

        for name, value in zip(self.status_fields, fields):
            func = self.field_maps.get(name, lambda x: x)
            status[name] = func(value)

        return status

    def _parse_nodelist_output(self, raw_output):
        pass
