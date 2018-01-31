from subprocess import Popen, PIPE, STDOUT
import os

def time_cmd(cmd, stdout=PIPE, stderr=STDOUT, envs=None):
    '''Return string output from a command line'''
    if type(cmd) == list:
        cmd = ' '.join(cmd)

    cmd = f'time ( {cmd} )'
    p = subprocess.Popen(cmd, shell=True, stdout=stdout,
                         stderr=stdout, env=envs)
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

def get_tail(fname, nlines=5, indent='    '):
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
