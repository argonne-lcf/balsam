import socket
import os
from compute_node import ComputeNode


class ThetaNode(ComputeNode):

    num_cpu = 64
    num_gpu = 0
    cpu_identifiers = list(range(num_cpu))
    gpu_identifiers = list(range(num_gpu))
    allow_multi_mpirun = True
    cpu_type = 'Intel KNL'
    cpu_mem_gb = 1000
    gpu_type = ''
    gpu_mem_gb = 0

    def __init__(self, node_id, hostname, job_mode=''):
        super(ThetaNode, self).__init__(node_id, hostname, job_mode)

    def __str__(self):
        return f'{self.hostname}:cpu{self.num_cpu}:gpu{self.num_gpu}'

    @classmethod
    def get_job_nodelist(cls):
        """
        Get all compute nodes allocated in the current job context
        """
        node_str = os.environ["COBALT_PARTNAME"]
        # string like: 1001-1005,1030,1034-1200
        node_ids = []
        ranges = node_str.split(',')
        for node_range in ranges:
            lo, *hi = node_range.split('-')
            lo = int(lo)
            if hi:
                hi = int(hi[0])
                node_ids.extend(list(range(lo, hi+1)))
            else:
                node_ids.append(lo)

        return [cls(node_id,f"nid{node_id:05d}") for node_id in node_ids]


if __name__ == '__main__':

    if 'COBALT_PARTNAME' not in os.environ:
        so.environ['COBALT_PARTNAME'] = '1001-1005,1030,1034-1200'

    print([ str(x) for x in ThetaNode.get_job_nodelist()])