import psutil

class DefaultNode:
    num_cpu = psutil.cpu_count()
    cpu_identifiers = list(range(num_cpu))
    
    @classmethod
    def get_job_nodelist(cls):
        return [cls('localhost')]
