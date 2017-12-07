from subprocess import Popen, PIPE, STDOUT
import os

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
