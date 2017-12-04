class DEFAULTMPICommand(object):
    '''Single node OpenMPI: ppn == num_ranks'''
    def __init__(self):
        self.mpi = 'mpirun'
        self.nproc = '-n'
        self.ppn = '-npernode'
        self.env = '-x'
        self.cpu_binding = None
        self.threads_per_rank = None
        self.threads_per_core = None

    def worker_str(self, workers):
        return ""

    def env_str(self, envs):
        envstrs = (f"{self.env} {var}={val}" for var,val in envs.items())
        return " ".join(envstrs)

    def threads(self, thread_per_rank, thread_per_core):
        result= ""
        if self.cpu_binding:
            result += f"{self.cpu_binding} "
        if self.threads_per_rank:
            result += f"{self.threads_per_rank} {thread_per_rank} "
        if self.threads_per_core:
            result += f"{self.threads_per_core} {thread_per_core} "
        return result

    def __call__(self, workers, *, app_cmd, num_ranks, ranks_per_node, envs,threads_per_rank=1,threads_per_core=1):
        '''Build the mpirun/aprun/runjob command line string'''
        workers = self.worker_str(workers)
        envs = self.env_str(envs)
        thread_str = self.threads(threads_per_rank, threads_per_core)
        result =  (f"{self.mpi} {self.nproc} {num_ranks} {self.ppn} "
                   f"{num_ranks} {envs} {workers} {thread_str} {app_cmd}")
        return result


class BGQMPICommand(DEFAULTMPICommand):
    def __init__(self):
        self.mpi = 'runjob'
        self.nproc = '--np'
        self.ppn = '-p'
        self.env = '--envs' # VAR1=val1:VAR2=val2
        self.cpu_binding = None
        self.threads_per_rank = None
        self.threads_per_core = None
    
    def worker_str(self, workers):
        if len(workers) != 1:
            raise BalsamRunnerException("BGQ requires exactly 1 worker (sub-block)")
        worker = workers[0]
        shape, block, corner = worker.shape, worker.block, worker.corner
        return f"--shape {shape} --block {block} --corner {corner} "

class CRAYMPICommand(DEFAULTMPICommand):
    def __init__(self):
        # 64 independent jobs, 1 per core of a KNL node: -n64 -N64 -d1 -j1
        self.mpi = 'aprun'
        self.nproc = '-n'
        self.ppn = '-N'
        self.env = '-e'
        self.cpu_binding = '-cc depth'
        self.threads_per_rank = '-d'
        self.threads_per_core = '-j'
    
    def worker_str(self, workers):
        if not workers:
            return ""
        return f"-L {','.join(worker.id for worker in workers)}"
