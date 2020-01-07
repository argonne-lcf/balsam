import subprocess

class LocalScheduler(Scheduler):

    def __init__(self):
        self._procs = {}
        self._files = {}
        self._job_id = 1

    def submit(self, script_path):
        new_id = self._start_cmd(cmd)
        return new_id

    def _start_cmd(self, cmdline, workdir):
        cur_id = self._job_id
        self._job_id += 1

        fname = Path(workdir).joinpath(cur_id + '.output')
        f = open(fname, 'wb')
        self._files[cur_id] = f
        
        p = subprocess.Popen(
            cmdline,
            stdout=f,
            stderr=subprocess.STDOUT
        )
        self._procs[cur_id] = p
        return cur_id
