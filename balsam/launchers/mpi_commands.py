class DefaultMPICommand(object):
    def __init__(self):
        self.mpi = 'mpirun'
        self.nproc = '-n'
        self.ppn = '-ppn'
        self.env = '-env'
        self.cpu_binding = None
        self.threads_per_rank = None
        self.threads_per_core = None

    def worker_str(self, workers):
        return ""

    def env_str(self, job):
        if job.environ_vars:
            return f"{self.env} {job.environ_vars}"
        return ""

    def threads(self, job):
        result= ""
        if self.cpu_binding:
            result += f"{self.cpu_binding} "
        if self.threads_per_rank:
            result += f"{self.threads_per_rank} {job.threads_per_rank} "
        if self.threads_per_core:
            result += f"{self.threads_per_core} {job.threads_per_core} "
        return result

    def __call__(self, job, workers, nproc=None):
        if nproc is None:
            nproc = job.num_nodes * job.processes_per_node
        workers = self.worker_str(workers)
        envs = self.env_str(job)
        result =  (f"{self.mpi} {self.nproc} {nproc} {self.ppn} "
                   "{job.processes_per_node} {envs} {workers} {threads} ")
        return result


class BGQMPICommand(DefaultMPICommand):
    def __init__(self):
        self.mpi = 'runjob'
        self.nproc = '--np'
        self.ppn = '-p'
        self.env = '--envs' # VAR1=val1:VAR2=val2
        self.cpu_binding = None
        self.threads_per_rank = None
        self.threads_per_core = None
    
    def env_str(self, job):
        if not job.environ_vars:
            return ""
        envs = job.environ_vars.split(':')
        result = ""
        for env in envs:
            result += f"{self.env} {env} "
        return result
    
    def worker_str(self, workers):
        if len(workers) != 1:
            raise BalsamRunnerException("BGQ requires exactly 1 worker (sub-block)")
        worker = workers[0]
        shape, block, corner = worker.shape, worker.block, worker.corner
        return f"--shape {shape} --block {block} --corner {corner} "

class CRAYMPICommand(DefaultMPICommand):
    def __init__(self):
        # 64 independent jobs, 1 per core of a KNL node: -n64 -N64 -d1 -j1
        self.mpi = 'aprun'
        self.nproc = '-n'
        self.ppn = '-N'
        self.env = '-e' # VAR1=val1:VAR2=val2
        self.cpu_binding = '-cc depth'
        self.threads_per_rank = '-d'
        self.threads_per_core = '-j'
    
    def env_str(self, job):
        if not job.environ_vars:
            return ""
        envs = job.environ_vars.split(':')
        result = ""
        for env in envs:
            result += f"{self.env} {env} "
        return result
    
    def worker_str(self, workers):
        if not workers:
            return ""
        return f"-L {','.join(worker.id for worker in workers)}"
