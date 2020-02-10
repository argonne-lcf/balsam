from .scheduler import SubprocessSchedulerInterface
import os,logging
logger = logging.getLogger(__name__)


# parse "00:00:00" to minutes
def parse_clock(t_str):
    parts = t_str.split(':')
    n = len(parts)
    H = M = S = 0
    if n == 3:
        H, M, S = map(int, parts)
    elif n == 2:
        M, S = map(int, parts)

    return H * 60 + M + round(S / 60)

# parse "1-00:00:00" to minutes
def parse_time_minutes(t_str):
    mins = 0
    try:
        print(t_str)
        parts = t_str.split('-')
        if len(parts) == 1:
            mins += parse_clock(parts[0])
        elif len(parts) == 2:
            mins += parse_clock(parts[1])
            mins += int(parts[0]) * 24 * 60
    except:
        return None
    else:
        return mins


class SlurmScheduler(SubprocessSchedulerInterface):
    status_exe = 'squeue'
    submit_exe = 'sbatch'
    delete_exe = 'scancel'
    nodelist_exe = 'sinfo'
    default_submit_kwargs = {}
    submit_kwargs_flag_map = {}

    # maps scheduler states to Balsam states
    job_states = {
        'PENDING': 'queued',
        'CONFIGURING': 'starting',
        'RUNNING': 'running',
        'COMPLETING': 'exiting',
        'RESV_DEL_HOLD': 'user_hold',
        'ADMIN_HOLD': 'admin_hold',
        'FINISHED': 'finished',
        'FAILED': 'failed',
        'CANCELLED': 'cancelled',
        'DEADLINE': 'finished',
        'PREEMPTED': 'finished',
        'REQUEUED': 'queued',
        'SIGNALING': 'exiting',
        'STAGE_OUT': 'exiting',
        'TIMEOUT': 'failed',
    }

    @staticmethod
    def _job_state_map(scheduler_state):
        return SlurmScheduler.job_states.get(scheduler_state, 'unknown')

    # maps Balsam status fields to the scheduler fields
    # should be a comprehensive list of scheduler status fields
    status_fields = {
        'id': 'jobid',
        'state': 'state',
        'wall_time_min': 'timelimit',
        'queue': 'partition',
        'nodes': 'numnodes',
        'project': 'account',
        'time_remaining_min': 'timeleft',
    }

    # when reading these fields from the scheduler apply
    # these maps to the string extracted from the output
    @staticmethod
    def _status_field_map(balsam_field):
        status_field_map = {
            'id': lambda id: int(id),
            'state': SlurmScheduler._job_state_map,
            'wall_time_min': parse_time_minutes,
            'nodes': lambda n: int(n),
            'time_remaining_min': parse_time_minutes,
            'backfill_time': parse_time_minutes,
        }
        return status_field_map.get(balsam_field, lambda x: x)

    # maps node list states to Balsam node states
    # descriptions: https://slurm.schedmd.com/sinfo.html
    node_states = {
        'alloc': 'busy',    # allocated
        'boot':  'busy',
        'comp':  'busy',    # completing
        'down':  'busy',
        'drain': 'busy',    # drained
        'drng':  'busy',    # draining
        'fail':  'busy',
        'failg': 'busy',    # failing
        'futr':  'busy',    # future
        'idle':  'idle',
        'maint': 'busy',    # maintenance
        'mix':   'busy',
        'npc':   'busy',    # perfctrs
        'pow_dn':'busy',    # power down
        'pow_up':'busy',    # power up
        'resv':  'busy',    # reserved
        'unk':   'busy',    # unknown
    }

    @staticmethod
    def _node_state_map(nodelist_state):
        try:
            # removing special symbols that have some meaning
            # in the future we might want to encode this info
            # * The node is presently not responding and will not be allocated any new work.
            nodelist_state = nodelist_state.replace('*','')
            # ~ The node is presently in a power saving mode
            nodelist_state = nodelist_state.replace('~','')
            # # The node is presently being powered up or configured.
            nodelist_state = nodelist_state.replace('#','')
            # % The node is presently being powered down.
            nodelist_state = nodelist_state.replace('%','')
            # $ The node is currently in a reservation with a flag value of "maintenance".
            nodelist_state = nodelist_state.replace('$','')
            # @ The node is pending reboot.
            nodelist_state = nodelist_state.replace('@','')
            # alloc+ The node is allocated to one or more active jobs plus one or more jobs are in the process of COMPLETING.
            nodelist_state = nodelist_state.replace('+','')
            return SlurmScheduler.node_states[nodelist_state]
        except KeyError:
            logger.warning('node state %s is not recognized',nodelist_state)
            return 'unknown'

    # maps the Balsam status fields to the node list fields
    # should be a comprehensive list of node list fields
    nodelist_fields = {
        'id': 'nodelist',
        'queues': 'partition',
        'node_state': 'stateshort',
    }

    fields_encondings = {
        'nodelist': '%N',
        'partition': '%P',
        'stateshort': '%t',
    }

    # when reading these fields from the scheduler apply
    # these maps to the string extracted from the output
    @staticmethod
    def _nodelist_field_map(balsam_field):
        nodelist_field_map = {
            'queues': lambda q: q.split(':'),
            'state': SlurmScheduler._node_state_map,
            'backfill_time': parse_time_minutes,
        }
        return nodelist_field_map.get(balsam_field, lambda x: x)

    def _get_envs(self):
        env = {}
        fields = self.status_fields.values()
        env['SQUEUE_FORMAT2'] = ','.join(fields)
        fields = self.nodelist_fields.values()
        env['SINFO_FORMAT'] = ' '.join(self.fields_encondings[field] for field in fields)
        return env

    def _render_submit_args(self, script_path, project, queue, num_nodes, time_minutes, **kwargs):
        args = [
            self.submit_exe,
            '-o', os.path.basename(os.path.splitext(script_path)[0]) + '.output',
            '-e', os.path.basename(os.path.splitext(script_path)[0]) + '.error',
            '-A', project,
            '-q', queue,
            '-N', str(int(num_nodes)),
            '-t', str(int(time_minutes)),
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
            args += ['-u', user]
        if project is not None:
            args += ['-A', project]
        if queue is not None:
            args += ['-q', queue]
        return args

    def _render_delete_args(self, job_id):
        return [self.delete_exe, str(job_id)]

    def _render_nodelist_args(self):
        return [self.nodelist_exe]

    def _parse_submit_output(self, submit_output):
        try:
            scheduler_id = int(submit_output)
        except ValueError:
            scheduler_id = int(submit_output.split()[-1])
        return scheduler_id

    def _parse_status_output(self, raw_output):
        # TODO: this can be much more efficient with a compiled regex findall()
        status_dict = {}
        print('stdout:',raw_output)
        job_lines = raw_output.strip().split('\n')[1:]
        for line in job_lines:
            print('line:',line)
            job_stat = self._parse_status_line(line)
            print('stat:',job_stat)
            if job_stat:
                id = int(job_stat['id'])
                status_dict[id] = job_stat
        return status_dict

    def _parse_status_line(self, line):
        status = {}
        fields = line.split()
        print(len(fields),len(self.status_fields))
        if len(fields) != len(self.status_fields):
            return status

        for name, value in zip(self.status_fields, fields):
            func = self._status_field_map(name)
            status[name] = func(value)
        print(status)
        return status

    def _parse_nodelist_output(self, stdout):
        raw_lines = stdout.split('\n')
        nodelist = {}
        node_lines = raw_lines[1:]
        for line in node_lines:
            self._parse_nodelist_line(line,nodelist)
        return nodelist

    def _parse_nodelist_line(self, line,nodelist):
        fields = line.split()
        if len(fields) != len(self.nodelist_fields):
            return

        node_ids = self._parse_node_field(fields[0])

        queue = fields[1]
        status = self._node_state_map(fields[2])

        for node_id in node_ids:
            if node_id in nodelist:
                nodelist[node_id]['queues'].append(queue)
                nodelist[node_id]['state'] = status
            else:
                nodelist[node_id] = {'queues':[queue],'state':status}

    @staticmethod
    def _parse_node_field(nodes_str):
        node_numbers_str = nodes_str[len('nid['):-1]
        node_ranges = node_numbers_str.split(',')
        node_ids = []
        for node_range in node_ranges:
            if '-' in node_range:
                parts = node_range.split('-')
                min = int(parts[0])
                max = int(parts[1])
                for i in range(min, max + 1):
                    node_ids.append(i)
            else:
                node_ids.append(int(node_range))

        return node_ids