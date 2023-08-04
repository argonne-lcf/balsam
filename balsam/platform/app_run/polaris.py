import logging
import os

from balsam.platform.compute_node import PolarisNode

from .app_run import SubprocessAppRun

logger = logging.getLogger(__name__)


class PolarisRun(SubprocessAppRun):
    """
    https://www.open-mpi.org/doc/v3.0/man1/mpiexec.1.php
    """

    def _build_cmdline(self) -> str:
        node_ids = [h for h in self._node_spec.hostnames]

        # If the user does not set a cpu_bind option,
        # this code sets cpu-bind to be optimal for the gpus being used.
        # This does not handle the case where the application is using less than
        # 8 cpus per gpu.  This code will not skip the appropriate number of cpus
        # in the rank binding assignments.
        if "cpu_bind" in self._launch_params.keys():
            cpu_bind = self._launch_params.get("cpu_bind")
        elif "--cpu-bind" in self._launch_params.keys():
            cpu_bind = self._launch_params.get("--cpu-bind")
        else:
            # Here we grab the cpu_ids assigned to the job in the NodeSpec object
            # If this is not set in NodeSpec (it is only set for single node jobs),
            # then we take the cpu_id list from the Polaris ComputeNode subclass,
            # assuming the job will have use of all the cpus in nodes assigned to it.
            cpu_ids = self._node_spec.cpu_ids[0]
            polaris_node = PolarisNode(self._node_spec.node_ids[0], self._node_spec.hostnames[0])
            if not cpu_ids:
                cpu_ids = polaris_node.cpu_ids

            cpus_per_rank = self.get_cpus_per_rank()

            # PolarisNode reverses the order of the gpu_ids, so assigning the cpu-bind
            # in ascending cpu order is what we want here.
            cpu_bind_list = ["list"]
            for irank in range(self._ranks_per_node):
                cpu_bind_list.append(":")
                for i in range(cpus_per_rank):
                    if i > 0:
                        cpu_bind_list.append(",")
                    cid = str(cpu_ids[i + cpus_per_rank * irank])
                    cpu_bind_list.append(cid)
                    # If the job is using 2 hardware threads per core, we need to add those threads to the list
                    # The additional threads should go in the same ascending order (threads 0 and 32 are on the
                    # same physical core, threads 31 and 63 are on the same physical core)
                    if self._threads_per_core == 2:
                        cpu_bind_list.append(",")
                        cid = str(cpu_ids[i + cpus_per_rank * irank] + len(polaris_node.cpu_ids))
                        cpu_bind_list.append(cid)
            cpu_bind = "".join(cpu_bind_list)

        launch_params = []
        for k in self._launch_params.keys():
            if k != "cpu_bind" and k != "--cpu-bind":
                launch_params.append(str(self._launch_params[k]))

        # The value of -d depends on the setting of cpu_bind.  If cpu-bind=core, -d is the number of
        # physical cores per rank, otherwise it is the number of hardware threads per rank
        # https://docs.alcf.anl.gov/running-jobs/example-job-scripts/
        depth = self._threads_per_rank
        if "core" == cpu_bind:
            depth = self.get_cpus_per_rank()

        nid_str = ",".join(map(str, node_ids))
        args = [
            "mpiexec",
            "-np",
            self.get_num_ranks(),
            "-ppn",
            self._ranks_per_node,
            "--hosts",
            nid_str,
            "--cpu-bind",
            cpu_bind,
            "-d",
            depth,
            *launch_params,
            self._cmdline,
        ]
        return " ".join(str(arg) for arg in args)

    def _set_envs(self) -> None:
        envs = os.environ.copy()
        envs.update(self._envs)

        # Here we grab the gpus assigned to the job from NodeSpec.  NodeSpec only
        # sets this for single node jobs.  For multinode jobs, gpu_ids below will
        # be an empty list of lists (e.g. [[], []]).  The ordering of the gpu_ids
        # is reversed in PolarisNode and therefore the reverse ordering of
        # cpus to gpus should be reflected here
        gpu_ids = self._node_spec.gpu_ids[0]
        cpu_ids = self._node_spec.cpu_ids[0]
        logger.info(f"Polaris set_envs: gpu_ids={gpu_ids} cpu_ids={cpu_ids}")

        # Here we set CUDA_VISIBLE_DEVICES for single node jobs only.  We assume
        # for multinode jobs that the job has access to all gpus, and
        # CUDA_VISIBLE_DEVICES is set by the user, for example by local rank with an
        # gpu_affinity.sh script that wraps around the user application in the
        # ApplicationDefinition.
        # One special case: if your job has one node, 2 ranks, and 1 gpu per rank, the
        # code here will set CUDA_VISIBLE_DEVICES to "3,2" or "1,0".  A user provided
        # gpu_affinity.sh script should take this assigment and use it to reset
        # CUDA_VISIBLE_DEVICES for each local rank.  The user script should NOT
        # round-robin the setting CUDA_VISIBLE_DEVICES starting from 3.
        if gpu_ids:
            envs["CUDA_DEVICE_ORDER"] = "PCI_BUS_ID"
            envs["CUDA_VISIBLE_DEVICES"] = ",".join(map(str, gpu_ids))
        envs["OMP_NUM_THREADS"] = str(self._threads_per_rank)
        self._envs = envs
