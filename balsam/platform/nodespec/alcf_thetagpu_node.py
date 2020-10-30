import socket
import os
from compute_node import ComputeNode


class ThetaGpuNode(ComputeNode):

    num_cpu = 128
    num_gpu = 8
    cpu_identifiers = list(range(num_cpu))
    gpu_identifiers = list(range(num_gpu))
    allow_multi_mpirun = True
    cpu_type = 'AMD EPYC 7742'
    cpu_mem_gb = 1000
    gpu_type = 'NVidia A100'
    gpu_mem_gb = 40

    def __init__(self, node_id, hostname, job_mode=''):
        super(ThetaGpuNode, self).__init__(node_id, hostname, job_mode)

    def __str__(self):
        return f'{self.hostname}:cpu{self.num_cpu}:gpu{self.num_gpu}'

    @classmethod
    def get_job_nodelist(cls):
        """
        Get all compute nodes allocated in the current job context
        """
        nodefile = os.environ["COBALT_NODEFILE"]
        # a file containing a list of node hostnames, one per line
        # thetagpu01
        # thetagpu02

        # read all lines of the file
        nodefile_lines = open(nodefile).readlines()
        # remove dangling newline character
        node_hostnames = [line.strip() for line in nodefile_lines]
        node_ids = [int(hostname[-2:]) for hostname in node_hostnames]

        return [cls(node_ids[i], node_hostnames[i]) for i in range(len(node_ids))]


if __name__ == '__main__':

    delete_tmp = False
    tmp_fn = 'tmp.nodefile'
    if 'COBALT_NODEFILE' not in os.environ:
        delete_tmp = True
        with open(tmp_fn, 'w') as file:
            file.write('thetagpu01\nthetagpu02\nthetagpu12\n')
        so.environ['COBALT_NODEFILE'] = os.path.join(os.getcwd(), tmp_fn)

    if delete_tmp:
        os.remove(tmp_fn)

    print([str(x) for x in ThetaGpuNode.get_job_nodelist()])
