import os

from .app_run import SubprocessAppRun

def _convert_gpu_bindings(bindings: list[str]) -> list[str]:
    """Convert Aurora GPU bindings between FLAT and COMPOSITE notation."""
    if not bindings:
        return []

    is_composite = ["." in binding for binding in bindings]
    if any(is_composite) and not all(is_composite):
        raise ValueError("Cannot mix FLAT and COMPOSITE bindings")

    if all(is_composite):
        # COMPOSITE gpu.tile -> FLAT tile
        converted = []
        for binding in bindings:
            gpu, tile = map(int, binding.split("."))

            if not 0 <= gpu < 6 or not 0 <= tile < 2:
                raise ValueError(f"Invalid COMPOSITE binding: {binding}")

            converted.append(str(2 * gpu + tile))

        return converted

class AuroraRun(SubprocessAppRun):
    """
    https://www.open-mpi.org/doc/v3.0/man1/mpiexec.1.php
    """

    def _build_cmdline(self) -> str:
        node_ids = [h for h in self._node_spec.hostnames]
        cpu_bind = self._launch_params.get("cpu_bind", "none")
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
            "--genvall",
            self._cmdline,
        ]
        return " ".join(str(arg) for arg in args)

    # FLAT tile -> COMPOSITE gpu.tile
    converted = []
    for binding in bindings:
        tile = int(binding)

        if not 0 <= tile < 12:
            raise ValueError(f"Invalid FLAT binding: {binding}")

        converted.append(f"{tile // 2}.{tile % 2}")

    return converted

    # Overide default because aurora/sunspot does not use CUDA
    def _set_envs(self) -> None:
        envs = os.environ.copy()
        envs.update(self._envs)
        envs["OMP_NUM_THREADS"] = str(self._threads_per_rank)

        # Check the assigned GPU ID list from the first compute node:
        gpu_ids = self._node_spec.gpu_ids[0]
        if gpu_ids:
            gpu_hierarchy_type = os.environ["ZE_FLAT_DEVICE_HIERARCHY"]
            selected_gpu_hierarchy_type = "COMPOSITE" if "." in gpu_ids[0] else "FLAT"
            if gpu_hierarchy_type != selected_gpu_hierarchy_type:
                gpu_ids = _convert_gpu_bindings(gpu_ids)
                    
            envs["ZE_ENABLE_PCI_ID_DEVICE_ORDER"] = "1"
            envs["ZE_AFFINITY_MASK"] = ",".join(map(str, gpu_ids))

        self._envs = envs
