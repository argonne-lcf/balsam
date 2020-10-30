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
    allow_multi_mpirun = True

    def __init__(self, node_id, hostname, job_mode):
        self.node_id = node_id
        self.hostname = hostname
        self.occupancy = 0.0
        self.cpu_occ = {label: 0.0 for label in self.cpu_identifiers}
        self.gpu_occ = {label: 0.0 for label in self.gpu_identifiers}
        self.available_cpu = float(self.num_cpu)
        self.available_gpu = float(self.num_gpu)
        self.tasks = {}

    def assign(self, task_label, *, num_cpus=0, num_gpus=0, occupancy=1.0):
        if task_label in self.tasks:
            raise ValueError(f"Already have task {task_label}")
        assigned_cpu_ids, assigned_gpu_ids = [], []
        return assigned_cpu_ids, assigned_gpu_ids

    def free(self, task_label):
        pass

    @staticmethod
    def get_remaining_time_min():
        return

    @classmethod
    def get_job_nodelist(cls):
        """
        Get all compute nodes allocated in the current job context
        """
        return [cls("localhost")]

