import os

from .app_run import SubprocessAppRun


class SummitJsrun(SubprocessAppRun):
    """
    https://docs.olcf.ornl.gov/systems/summit_user_guide.html#running-jobs
    """

    def _build_cmdline(self) -> str:
        cpu_per_rs = self.get_cpus_per_rank()
        args = [
            "jsrun",
            "--nrs",  # number of Resource Sets == number of ranks
            self.get_num_ranks(),
            "--tasks_per_rs",  # ranks per resource set
            1,  # 1 rank per resource set (fixed assumption)
            "--rs_per_host",
            self._ranks_per_node,
            "--cpu_per_rs",
            cpu_per_rs,  # number of cores per resource set
            "--bind",
            "rs",  # Bind to all cores of resource set
            "--gpu_per_rs",
            self._gpus_per_rank,  # gpus per resource set
            self._cmdline,
        ]
        return " ".join(str(arg) for arg in args)

    def _set_envs(self) -> None:
        # Override because we do not want/need to set
        # CUDA_VISIBLE_DEVICES by default on Summit
        envs = os.environ.copy()
        envs.update(self._envs)
        envs["OMP_NUM_THREADS"] = str(self._threads_per_rank)
        self._envs = envs
