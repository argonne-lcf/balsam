class ComputeNode:

    cpu_ids = []
    gpu_ids = []

    def __init__(self, node_id, hostname):
        self.node_id = node_id
        self.hostname = hostname
        self.occupancy = 0.0
        self.jobs = {}
        self.idle_cpus = [i for i in self.cpu_ids]
        self.busy_cpus = []
        self.idle_gpus = [i for i in self.gpu_ids]
        self.busy_gpus = []

    def check_fit(self, num_cpus, num_gpus, occupancy):
        if self.occupancy + occupancy > 1.001:
            return False
        elif num_cpus > len(self.idle_cpus):
            return False
        elif num_gpus > len(self.idle_gpus):
            return False
        else:
            return True

    def assign(self, job_id, num_cpus=0, num_gpus=0, occupancy=1.0):
        if job_id in self.jobs:
            raise ValueError(f"Already have job {job_id}")

        self.occupancy += occupancy
        if self.occupancy > 0.999:
            self.occupancy = 1.0

        assigned_cpus = self.idle_cpus[:num_cpus]
        assigned_gpus = self.idle_gpus[:num_gpus]
        if assigned_cpus:
            self.busy_cpus.extend(assigned_cpus)
            self.idle_cpus = [i for i in self.idle_cpus if i not in assigned_cpus]
        if assigned_gpus:
            self.busy_gpus.extend(assigned_gpus)
            self.idle_gpus = [i for i in self.idle_gpus if i not in assigned_gpus]
        resource_spec = {
            "cpu_ids": assigned_cpus,
            "gpu_ids": assigned_gpus,
            "occupancy": occupancy,
        }
        self.jobs[job_id] = resource_spec
        return resource_spec

    def free(self, job_id):
        resource_spec = self.jobs.pop(job_id)
        self.occupancy -= resource_spec["occupancy"]
        if self.occupancy < 0.001:
            self.occupancy = 0.0
        cpu_ids = resource_spec["cpu_ids"]
        gpu_ids = resource_spec["gpu_ids"]
        if cpu_ids:
            self.idle_cpus.extend(cpu_ids)
            self.busy_cpus = [i for i in self.busy_cpus if i not in cpu_ids]
        if gpu_ids:
            self.idle_gpus.extend(gpu_ids)
            self.busy_gpus = [i for i in self.busy_gpus if i not in gpu_ids]

    @classmethod
    def get_job_nodelist(cls, job_mode):
        """
        Get all compute nodes allocated in the current job context
        """
        return []

    @staticmethod
    def get_batch_job_id():
        return None

    def __repr__(self):
        return f"{self.__class__.__name__}(id={self.node_id}, hostname={self.hostname})"
