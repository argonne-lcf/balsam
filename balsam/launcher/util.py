from subprocess import Popen, PIPE, STDOUT
import os
from threading import Thread
from django.conf import settings
import time

def time_cmd(cmd, stdout=PIPE, stderr=STDOUT, envs=None):
    '''Return string output from a command line'''
    if type(cmd) == list:
        cmd = ' '.join(cmd)

    cmd = f'time ( {cmd} )'
    p = subprocess.Popen(cmd, shell=True, stdout=stdout,
                         stderr=stdout, env=envs,
                         executable='/bin/bash')
    stdout = p.communicate()[0].decode('utf-8')
    real_seconds = parse_real_time(stdout)
    return stdout, realtime

def parse_real_time(stdout):
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

def get_tail(fname, nlines=16, indent='    '):
    '''grab last nlines of fname and format nicely'''

    # Easier to ask OS than implement a proper tail
    proc = Popen(f'tail -n {nlines} {fname}'.split(),stdout=PIPE, 
                 stderr=STDOUT)
    tail = proc.communicate()[0].decode()
    lines = tail.split('\n')
    for i, line in enumerate(lines[:]):
        lines[i] = indent + line
    return '\n'.join(lines)

class cd:
    '''Context manager for changing cwd'''
    def __init__(self, new_path):
        self.new_path = os.path.expanduser(new_path)

    def __enter__(self):
        self.saved_path = os.getcwd()
        os.chdir(self.new_path)

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            print(exc_type, exc_value, traceback)
        os.chdir(self.saved_path)

class MonitorStream(Thread):
    '''Thread: non-blocking read of a process's stdout'''
    def __init__(self, runner_output):
        super().__init__()
        self.stream = runner_output
        self.queue = Queue()
        self.daemon = True

    def run(self):
        # Call readline until empty string is returned
        for line in iter(self.stream.readline, b''):
            self.queue.put(line.decode('utf-8'))
        self.stream.close()

    def available_lines(self):
        while True:
            try: yield self.queue.get_nowait()
            except Empty: return

def delay_generator(period=settings.SERVICE_PERIOD):
    '''Generator: Block for ``period`` seconds since the last call to __next__()'''
    nexttime = time.time() + period
    while True:
        now = time.time()
        tosleep = nexttime - now
        if tosleep <= 0:
            nexttime = now + period
        else:
            time.sleep(tosleep)
            nexttime = now + tosleep + period
        yield

def elapsed_time_minutes():
    '''Generator: yield elapsed time in minutes since first call to __next__'''
    start = time.time()
    while True:
        yield (time.time() - start) / 60.0

def remaining_time_minutes(time_limit_minutes=0.0):
    '''Generator: yield remaining time for Launcher execution

    If time_limit_minutes is given, use internal timer to count down remaining
    time. Otherwise, query scheduler for remaining time.

    Args:
        - ``time_limit_minutes`` (*float*): runtime limit
    Yields:
        - ``remaining`` (*float*): remaining time
    Raises:
        - ``StopIteration``: when there is no time left
    '''
    if time_limit_minutes > 0.0:
        elapsed_timer = elapsed_time_minutes()
        get_remaining = lambda: time_limit_minutes - next(elapsed_timer)
    else:
        from balsam.service.schedulers import JobEnv
        get_remaining = lambda: JobEnv.remaining_time_seconds() / 60.0

    while True:
        remaining_min = get_remaining()
        if remaining_min > 0: yield remaining_min
        else: raise StopIteration
