import logging
import os
import stat

from balsam.platform.compute_node.alcf_polaris_node import PolarisNode

from .app_run import SubprocessAppRun

logger = logging.getLogger(__name__)


class PolarisRun(SubprocessAppRun):
    """
    https://www.open-mpi.org/doc/v3.0/man1/mpiexec.1.php
    """

    def _build_cmdline(self) -> str:
        node_ids = [h for h in self._node_spec.hostnames]

        # cms21: currently this is broken for multinode jobs

        cpu_bind = self._launch_params.get("cpu_bind", "none")
        gpu_affinity_script = ""
        if cpu_bind == "none" and self._gpus_per_rank > 0:
            if len(self._node_spec.node_ids) == 1 or self._ranks_per_node == 1:
                cpu_ids = self._node_spec.cpu_ids[0]
                gpu_ids = self._node_spec.gpu_ids[0]
            else:
                gpu_ids = self._envs["CUDA_VISIBLE_DEVICES"].split(
                    ","
                )  # These should be distributed across local ranks
                polaris_node = PolarisNode(self._node_spec.node_ids[0], self._node_spec.hostnames[0])
                cpu_ids = polaris_node.cpu_ids
                node_gpu_ids = polaris_node.gpu_ids
                gpu_affinity_script = self._cwd.joinpath("set_affinity_gpu_polaris.sh")
                with open(gpu_affinity_script, "w") as f:
                    f.write(
                        f"""#!/bin/bash -l
                                    gpu_ids=( "{" ".join(gpu_ids)}" )
                                    num_gpus={len(node_gpu_ids)}
                                    gpus_per_rank={self._gpus_per_rank}
                                    ngpu=0
                                    gpu_string=""\n
                                    """
                    )
                    f.write(
                        """while [ $ngpu -lt $gpus_per_rank ]
                                do
                                    igpu=$(((${PMI_LOCAL_RANK} * ${gpus_per_rank}) + ${ngpu} % ${num_gpus}))
                                    gpu=${gpu_ids[$igpu]}
                                    ##gpu=$((${num_gpus} - 1 - ${ngpu} - (${PMI_LOCAL_RANK} * ${gpus_per_rank}) % ${num_gpus}))
                                    sep=""
                                    if [ $ngpu -gt 0 ]
                                    then
                                        sep=","
                                    fi
                                    gpu_string=$gpu_string$sep$gpu
                                    ngpu=$((${igpu} + 1))
                                done
                                export CUDA_VISIBLE_DEVICES=$gpu_string
                                echo “RANK= ${PMI_RANK} LOCAL_RANK= ${PMI_LOCAL_RANK} gpu= $gpu_string”
                                exec "$@"
                    """
                    )
                    st = os.stat(gpu_affinity_script)
                    os.chmod(gpu_affinity_script, st.st_mode | stat.S_IEXEC)

                # gpu_ids = polaris_node.gpu_ids
                # num_gpus = len(gpu_ids)
                # gpu_affinity_script = self._cwd.joinpath("set_affinity_gpu_polaris.sh")
                # with open(gpu_affinity_script,"w") as f:
                #     f.write(f"""#!/bin/bash -l
                #                 num_gpus={num_gpus}
                #                 gpus_per_rank={self._gpus_per_rank}\n"""+
                #              """gpu=$((${num_gpus} - 1 - ${PMI_LOCAL_RANK} % ${num_gpus}))\n
                #                 export CUDA_VISIBLE_DEVICES=$gpu\n
                #                 echo “RANK= ${PMI_RANK} LOCAL_RANK= ${PMI_LOCAL_RANK} gpu= ${gpu}”\n
                #                 exec "$@"\n
                #             """)
                #     st = os.stat(gpu_affinity_script)
                #     os.chmod(gpu_affinity_script, st.st_mode | stat.S_IEXEC)

            cpus_per_rank = self.get_cpus_per_rank()
            cpu_ids_ns = self._node_spec.cpu_ids

            cpu_bind_list = ["verbose,list"]
            for irank in range(self._ranks_per_node):
                cpu_bind_list.append(":")
                for i in range(cpus_per_rank):
                    if i > 0:
                        cpu_bind_list.append(",")
                    cid = str(cpu_ids[i + cpus_per_rank * irank])
                    cpu_bind_list.append(cid)
            cpu_bind = "".join(cpu_bind_list)
            logger.info(
                f"Polaris app_run: cpu_bind={cpu_bind} cpu_ids={cpu_ids} cpu_ids_ns={cpu_ids_ns} gpu_ids={gpu_ids}"
            )

        launch_params = []
        for k in self._launch_params.keys():
            if k != "cpu_bind":
                launch_params.append("--" + k)
                launch_params.append(str(self._launch_params[k]))

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
            self._threads_per_rank,
            *launch_params,
            gpu_affinity_script,
            self._cmdline,
        ]
        return " ".join(str(arg) for arg in args)

    # Overide default because sunspot does not use CUDA
    def _set_envs(self) -> None:

        envs = os.environ.copy()
        envs.update(self._envs)
        # Check the assigned GPU ID list from the first compute node:
        gpu_ids = self._node_spec.gpu_ids[0]
        cpu_ids = self._node_spec.cpu_ids[0]
        logger.info(f"Polaris set_envs: gpu_ids={gpu_ids} cpu_ids={cpu_ids}")
        if gpu_ids:
            envs["CUDA_DEVICE_ORDER"] = "PCI_BUS_ID"
            envs["CUDA_VISIBLE_DEVICES"] = ",".join(map(str, gpu_ids))
        else:
            polaris_node = PolarisNode(self._node_spec.node_ids[0], self._node_spec.hostnames[0])
            if self._gpus_per_rank > 0:
                gpu_ids = polaris_node.gpu_ids[0 : self.get_gpus_per_node_for_job()]
                envs["CUDA_DEVICE_ORDER"] = "PCI_BUS_ID"
                envs["CUDA_VISIBLE_DEVICES"] = ",".join(map(str, gpu_ids))

        envs["OMP_NUM_THREADS"] = str(self._threads_per_rank)
        self._envs = envs
