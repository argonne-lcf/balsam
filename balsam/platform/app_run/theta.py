import time
from .app_run import SubprocessAppRun


class ThetaAprun(SubprocessAppRun):
    """
    https://www.alcf.anl.gov/support-center/theta/running-jobs-and-submission-scripts
    """

    def _pre_popen(self):
        time.sleep(0.01)

    def _build_cmdline(self):
        node_ids = [n["node_id"] for n in self._node_spec.node_ids]
        nid_str = ",".join(map(str, node_ids))
        num_ranks = self._ranks_per_node * len(self._node_spec.node_ids)
        cpu_affinity = self._launch_params.get("cpu_affinity", "none")
        if cpu_affinity not in ["none", "depth"]:
            cpu_affinity = "none"
        args = [
            "aprun",
            "-n",
            num_ranks,
            "-N",
            self._ranks_per_node,
            "-L",
            nid_str,
            "-cc",
            cpu_affinity,
            "-d",
            self._threads_per_rank,
            "-j",
            self._threads_per_core,
            self._cmdline,
        ]
        return " ".join(args)
