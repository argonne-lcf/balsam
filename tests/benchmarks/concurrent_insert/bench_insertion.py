import os
import sys
from socket import gethostname
import time
import subprocess
from importlib.util import find_spec

from balsam.service.models import BalsamJob
from tests.BalsamTestCase import BalsamTestCase, cmdline
from tests.BalsamTestCase import poll_until_returns_true
from tests.BalsamTestCase import create_job, create_app

def get_real_time(stdout):
    '''Parse linux "time" command'''
    if type(stdout) == bytes:
        stdout = stdout.decode()

    lines = stdout.split('\n')
    nlines = len(lines)
    nlines = max(5, nlines)

    real_line = None
    for line in lines[-nlines:]:
        if line.startswith('real') and len(line.split())==2:
            real_line = line
            break

    if not real_line: return None

    time_str = real_line.split()[1]

    minutes, seconds = time_str.split('m')
    minutes = float(minutes)
    seconds = float(seconds[:-1])
    return 60*minutes + seconds
    
class TestInsertion(BalsamTestCase):
    def setUp(self):
        hello = find_spec("tests.benchmarks.concurrent_insert.hello").origin
        create_app(name="hello", executable=hello)

        data_dir = find_spec("tests.benchmarks.data").origin
        self.data_dir = os.path.dirname(data_dir)

    def test_concurrent_mpi_insert(self):
        '''Timing: many MPI ranks simultaneously call dag.add_job'''
        resultpath = os.path.join(self.data_dir, 'concurrent_insert.dat')

        num_nodes = int(os.environ.get('COBALT_JOBSIZE', 0))
        if num_nodes < 1:
            self.skipTest("Need a COBALT allocation")

        cobalt_envs = {k:v for k,v in os.environ.items() if 'COBALT' in k}
        with open(resultpath, 'a') as fp:
            fp.write(f'# BENCHMARK: test_concurrent_mpi_insert ({__file__})\n')
            fp.write(f'# Host: {gethostname()}\n')
            for k, v in cobalt_envs.items():
                fp.write(f'# {k}: {v}\n')
            fp.write("# Each rank simultaneously calls dag.add_job (num_ranks simultaneous insertions)\n")

        ranks_per_node = [1, 2, 4, 8, 16, 32]
        python = sys.executable
        insert_app = find_spec("tests.benchmarks.concurrent_insert.mpi_insert").origin
        
        with open(resultpath, 'a') as fp:
            header = f'# {"# ranks".rjust(14):14} {"time / seconds".rjust(16):16} {"py_time / seconds".rjust(18):18}'
            header += '\n# ' + '-'*(len(header)-2) + '\n'
            fp.write(header)

        for rpn in ranks_per_node:
            for job in BalsamJob.objects.all(): job.delete()

            total_ranks = num_nodes * rpn
            start = time.time()

            cmdline = f"time aprun -n {total_ranks} -N {rpn} {python} {insert_app}"
            proc = subprocess.Popen(cmdline, shell=True, stdout=subprocess.PIPE, 
                    stderr=subprocess.STDOUT)
            stdout, stderr = proc.communicate()
            elapsed_py = time.time() - start
            elapsed_sh = get_real_time(stdout)
            
            out_lines = stdout.decode().split('\n')
            success = list(l for l in out_lines if 'added job: success' in l)
            self.assertEqual(len(success), total_ranks)
            self.assertEqual(BalsamJob.objects.count(), total_ranks)
        
            with open(resultpath, 'a') as fp:
                fp.write(f'{total_ranks:16} {elapsed_sh:16.3f} {elapsed_py:18.3f}\n')

        with open(resultpath, 'a') as fp: fp.write(f'\n')
