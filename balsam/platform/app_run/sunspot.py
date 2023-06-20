import os

from .app_run import SubprocessAppRun

from balsam.platform.compute_node import SunspotNode

class SunspotRun(SubprocessAppRun):
    """
    https://www.open-mpi.org/doc/v3.0/man1/mpiexec.1.php
    """

    def _build_cmdline(self) -> str:
        node_ids = [h for h in self._node_spec.hostnames]
        
        nid_str = ",".join(map(str, node_ids))

        if "cpu_bind" in self._launch_params.keys():
            cpu_bind = self._launch_params.get("cpu_bind", "none")
        else:
            # Here we grab the cpu_ids assigned to the job in the NodeSpec object
            # If this is not set in NodeSpec (it is only set for single node jobs),
            # then we take the cpu_id list from the Sunspot ComputeNode subclass,
            # assuming the job will have use of all the cpus in nodes assigned to it.
            cpu_ids_ns = self._node_spec.cpu_ids[0]
            if cpu_ids_ns:
                cpu_ids = self._node_spec.cpu_ids[0]
                if self._threads_per_core == 2:
                    sunspot_node = SunspotNode(self._node_spec.node_ids[0], self._node_spec.hostnames[0])
            else:
                sunspot_node = SunspotNode(self._node_spec.node_ids[0], self._node_spec.hostnames[0])
                cpu_ids = sunspot_node.cpu_ids

            cpus_per_rank = self.get_cpus_per_rank()

            cpu_bind_list = ["verbose,list"]
            for irank in range(self._ranks_per_node):
                cpu_bind_list.append(":")
                for i in range(cpus_per_rank):
                    if i > 0:
                        cpu_bind_list.append(",")
                    cid = str(cpu_ids[i + cpus_per_rank * irank])
                    cpu_bind_list.append(cid)
                    # If the job is using 2 hardware threads per core, we need to add those threads to the list
                    # The additional threads should go in the same ascending order (threads 0 and 105 are on the
                    # same physical core, threads 104 and 207 are on the same physical core)
                    if self._threads_per_core == 2:
                        cpu_bind_list.append(",")
                        cid = str(cpu_ids[i + cpus_per_rank * irank] + len(sunspot_node.cpu_ids))
                        cpu_bind_list.append(cid)
            cpu_bind = "".join(cpu_bind_list)

        launch_params = []
        for k in self._launch_params.keys():
            if k != "cpu_bind":
                launch_params.append("--" + k)
                launch_params.append(str(self._launch_params[k]))

        # The value of -d depends on the setting of cpu_bind.  If cpu-bind=core, -d is the number of
        # physical cores per rank, otherwise it is the number of hardware threads per rank
        # https://docs.alcf.anl.gov/running-jobs/example-job-scripts/
        depth = self._threads_per_rank
        if "core" in cpu_bind:
            depth = self.get_cpus_per_rank()

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
            "--envall",
            self._cmdline,
        ]
        return " ".join(str(arg) for arg in args)

    # Overide default because sunspot does not use CUDA
    def _set_envs(self) -> None:
        envs = os.environ.copy()
        envs.update(self._envs)
        envs["OMP_NUM_THREADS"] = str(self._threads_per_rank)

        # Check the assigned GPU ID list from the first compute node:
        gpu_ids = self._node_spec.gpu_ids[0]
        if gpu_ids:
            envs["ZE_ENABLE_PCI_ID_DEVICE_ORDER"] = "1"
            envs["ZE_AFFINITY_MASK"] = ",".join(map(str, gpu_ids))

        self._envs = envs
