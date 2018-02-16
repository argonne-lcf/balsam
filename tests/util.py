import getpass
import os
import subprocess
import signal
from socket import gethostname
import time
from importlib.util import find_spec

DATA_DIR = find_spec("tests.benchmarks.data").origin
DATA_DIR = os.path.dirname(DATA_DIR)

def launcher_info(num_workers=None, max_ranks=None):
    from balsam.service.schedulers import Scheduler
    from balsam.launcher import worker
    from balsam.launcher.launcher import get_args
    from balsam.launcher import mpi_commands

    args = '--consume-all '
    if num_workers and num_workers > 0:
        args += f'--num-workers {num_workers} '

    if max_ranks and max_ranks > 0:
        args += f'--max-ranks-per-node {max_ranks} '

    config = get_args(args.split())
    scheduler = Scheduler.scheduler_main
    group = worker.WorkerGroup(config, host_type=scheduler.host_type,
                               workers_str=scheduler.workers_str,
                               workers_file=scheduler.workers_file)
    host_type = scheduler.host_type
    num_workers = scheduler.num_workers or 1
        
    mpi_cmd_class = getattr(mpi_commands, f"{host_type}MPICommand")
    mpi_cmd = mpi_cmd_class()

    class LaunchInfo: pass
    info = LaunchInfo()
    info.parsed_args = config
    info.host_type = host_type
    info.workerGroup = group
    info.scheduler = scheduler
    info.num_workers = num_workers
    info.mpi_cmd = mpi_cmd

    return info

def get_real_time(stdout):
    '''Parse linux "time -p" command'''
    if type(stdout) == bytes:
        stdout = stdout.decode()

    lines = stdout.split('\n')

    real_lines = [l for l in lines[-5:] if l.startswith('real')]
    if not real_lines:
        return None
    elif len(real_lines) > 1:
        real_line = real_lines[-1]
    else:
        real_line = real_lines[0]

    time_str = real_line.split()[1]
    return float(time_str)
            
def poll_until_returns_true(function, *, args=(), period=1.0, timeout=12.0):
    start = time.time()
    result = False
    while time.time() - start < timeout:
        result = function(*args)
        if result: break
        else: time.sleep(period)
    return result

def cmdline(cmd,envs=None):
    '''Return string output from a command line'''
    if type(cmd) == list:
        cmd = ' '.join(cmd)

    cmd = f'time -p ( {cmd} )'
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT,env=envs,
                         executable='/bin/bash')
    stdout = p.communicate()[0].decode('utf-8')
    realtime = get_real_time(stdout)
    return stdout, realtime

def ls_procs(keywords):
    if type(keywords) == str: 
        keywords = keywords.split()

    username = getpass.getuser()
    
    searchcmd = 'ps aux | grep '
    searchcmd += ' | grep '.join(f'"{k}"' for k in keywords) 
    stdout, _ = cmdline(searchcmd)

    processes = [line for line in stdout.split('\n') if 'python' in line and line.split()[0]==username]
    return processes

def sig_processes(process_lines, signal):
    for line in process_lines:
        proc = int(line.split()[1])
        try: 
            os.kill(proc, signal)
        except ProcessLookupError:
            pass

def stop_processes(name):
    processes = ls_procs(name)
    sig_processes(processes, signal.SIGTERM)
    
    def check_processes_done():
        procs = ls_procs(name)
        return len(procs) == 0

    poll_until_returns_true(check_processes_done, period=2, timeout=12)
    processes = ls_procs(name)
    if processes:
        sig_processes(processes, signal.SIGKILL)
        time.sleep(3)

def stop_launcher_processes():
    stop_processes('launcher.py')

def run_launcher_until(function, args=(), period=1.0, timeout=60.0, maxrpn=8):
    cmd = f'balsam launcher --consume --max-ranks-per-node {maxrpn}'
    launcher_proc = subprocess.Popen(cmd.split(),
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT,
                                     )
    success = poll_until_returns_true(function, args=args, period=period, timeout=timeout)
    stop_launcher_processes()
    return success

def run_launcher_until_state(job, state, period=1.0, timeout=60.0):
    def check():
        job.refresh_from_db()
        return job.state == state
    success = run_launcher_until(check, period=period, timeout=timeout)
    return success

class FormatTable:
    def __init__(self, columns):
        self.columns = columns
        assert len(columns) == len(set(columns))
        self.widths = {c : max(8, len(c)+4) for c in columns}
        self.rows = []

    def add_row(self, **kwargs):
        assert set(kwargs.keys()) == set(self.columns)
        row = [kwargs[c] for c in self.columns]
        for i, field in enumerate(row[:]):
            if type(field) == float: row[i] = "%.3f" % field
            if type(field) == int:   row[i] = "%d" % field

        for col, field in zip(self.columns, row):
            self.widths[col] = max(self.widths[col], len(field)+4)
        self.rows.append(row)

    @staticmethod
    def create_header(title, comment):
        from django.conf import settings
        header = ''
        cobalt_envs = {k:v for k,v in os.environ.items() if 'COBALT' in k}
        header += f'# BENCHMARK: {title}\n'
        header += f'# Host: {gethostname()}\n'
        for k, v in cobalt_envs.items():
            header += f'# {k}: {v}\n'
        for k, v in settings.DATABASES['default'].items():
            header += f'# {k}: {v}\n'
        header += f"# {comment}\n"
        return header

    def generate(self, title, comment):
        table = ''
        table += self.create_header(title, comment)

        labels = "".join(col.rjust(self.widths[col]) for col in self.columns)
        labels = f"# {labels}\n# " + "-"*len(labels)+"\n"
        table += labels

        for row in self.rows:
            table += "  "
            table += "".join(field.rjust(self.widths[col]) for field, col in
                              zip(row, self.columns))
            table += "\n"
        return table+"\n"

def process_job_times(time0=None):
    '''Return a dict of {state : [list_of_seconds_for_each_job_to_reach_state]}
    Useful for building CDF of completion times and profiling job processing throughput'''
    from balsam.service.models import TIME_FMT, BalsamJob, STATE_TIME_PATTERN
    from collections import defaultdict
    from datetime import datetime

    data = BalsamJob.objects.values_list('state_history', flat=True)
    data = '\n'.join(data)
    matches = STATE_TIME_PATTERN.finditer(data)
    result = ( m.groups() for m in matches )
    result = ( (state, datetime.strptime(time_str, TIME_FMT))
              for (time_str, state) in result )
    
    time_data = defaultdict(list)
    for state, time in result:
        time_data[state].append(time)

    if time0 is None: time0 = min(time_data['READY'])

    for state in time_data.keys():
        time_data[state] = [(t - time0).total_seconds() for t in sorted(time_data[state])]

    return time_data

def print_jobtimes_cdf(job_times):
    import numpy as np

    ready_times = np.array(job_times['READY'])
    pre_times = np.array(job_times['PREPROCESSED'])
    rundone_times = np.array(job_times['RUN_DONE'])
    jobfin_times = np.array(job_times['JOB_FINISHED'])

    max_time = round(max(jobfin_times) + 1.5)
    time_grid = np.arange(0, max_time, 0.1)

    count_ready = [(ready_times <= time_grid[i]).sum() for i in range(len(time_grid))]
    count_pre = [(pre_times <= time_grid[i]).sum() for i in range(len(time_grid))]
    count_rundone = [(rundone_times <= time_grid[i]).sum() for i in range(len(time_grid))]
    count_jobfin = [(jobfin_times <= time_grid[i]).sum() for i in range(len(time_grid))]

    result = "# Time num_ready num_preprocessed num_run_done num_job_finished\n"
    for time, *counts in zip(time_grid, count_ready, count_pre, count_rundone, count_jobfin):
        result += f'{time:8.3f} {" ".join("%3d" % c for c in counts)}\n'
    return result

def benchmark_outfile_path(file_basename):
    base = os.path.join(DATA_DIR, file_basename)
    resultpath = base
    i = 1
    while os.path.exists(resultpath):
        resultpath = f"{base}.{i}"
        i += 1
    return resultpath
