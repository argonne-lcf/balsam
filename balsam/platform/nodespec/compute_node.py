# MPI job mode: apps on < 1 node pack; otherwise assume a whole number of nodes (cannot take 1.5 nodes)
    # <1 node:
    #    assign(full_node=False, pass requested_cpus and requested_gpus
# Serial job mode: all apps run on <=1 nodes; always deal with packing
# Script job mode: reserves nodes, but run app as local subprocess with env-vars (not via MPIRun)
class ComputeNode:

    num_cpu = 4
    num_gpu = 0
    cpu_identifiers = list(range(4))
    gpu_identifiers = []
    multi_app_per_node = True

    def __init__(self, hostname, job_mode):
        self.hostname = hostname

        self.cpu_states = {label : 0.0 for label in self.cpu_identifiers}
        self.gpu_states = {label : 0.0 for label in self.gpu_identifiers}
        self.available_cpu = float(self.num_cpu)
        self.available_gpu = float(self.num_gpu)
        self.tasks = {}

    def assign(self, task_label, *, full_node=True, requested_cpu=0, requested_gpu=0):
        if not self.multi_app_per_node:
            if self.cpu_occ + self.gpu_occ > 0:
                raise ValueError('Node is partially occupied')
            self.cpu_occ = self.num_cpu
            self.gpu_occ = self.num_gpu
            return

        if requested_cpu > self.available_cpu:
            raise ValueError("Insufficient CPU")
        if requested_gpu > self.available_gpu:
            raise ValueError("Insufficient GPU")
        free_cpu = 

    def free(self, task_label):

    @classmethod
    def get_job_nodelist(cls):
        """
        Get all compute nodes allocated in the current job context
        """
        return [cls('localhost')]
