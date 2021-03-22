import logging
import os
import subprocess
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import IO, Dict, List, Optional, Union, cast

import psutil  # type: ignore

from balsam.site.launcher import NodeSpec

logger = logging.getLogger(__name__)


class _MockProcess:
    def cpu_affinity(self, cpus: Optional[List[int]] = None) -> None:
        pass


_psutil_process = psutil.Process()
try:
    _psutil_process.cpu_affinity()
except AttributeError:
    _psutil_process = _MockProcess()
    logger.debug("No psutil cpu_affinity support")


class TimeoutExpired(subprocess.TimeoutExpired):
    pass


class AppRun(ABC):
    """
    Balsam-wide Application launch interface
    The API defines the spec of an App invocation and manages
    its lifecycle: start/poll/kill/tail_output, etc...
    """

    def __init__(
        self,
        cmdline: str,
        preamble: Union[None, str, List[str]],
        envs: Dict[str, str],
        cwd: Path,
        outfile_path: Path,
        node_spec: NodeSpec,
        ranks_per_node: int,
        threads_per_rank: int,
        threads_per_core: int,
        launch_params: Dict[str, str],
        gpus_per_rank: int,
    ):
        self._cmdline = cmdline
        self._preamble = preamble
        self._envs = envs
        self._cwd = cwd
        self._outfile_path = outfile_path
        self._node_spec = node_spec
        self._ranks_per_node = ranks_per_node
        self._threads_per_rank = threads_per_rank
        self._threads_per_core = threads_per_core
        self._launch_params = launch_params
        self._gpus_per_rank = gpus_per_rank

    def get_num_ranks(self) -> int:
        return self._ranks_per_node * len(self._node_spec.node_ids)

    def get_cpus_per_rank(self) -> int:
        cpu_per_rank = len(self._node_spec.cpu_ids[0]) // self._ranks_per_node
        if not cpu_per_rank:
            cpu_per_rank = max(1, int(self._threads_per_rank // self._threads_per_core))
        return cpu_per_rank

    @abstractmethod
    def start(self) -> None:
        pass

    @abstractmethod
    def poll(self) -> Optional[int]:
        pass

    @abstractmethod
    def terminate(self) -> None:
        pass

    @abstractmethod
    def kill(self) -> None:
        pass

    @abstractmethod
    def tail_output(self, nlines: int = 10) -> str:
        pass

    @abstractmethod
    def wait(self, timeout: Optional[float] = None) -> int:
        pass


class FailedStartProcess:
    def poll(self) -> Optional[int]:
        return 12345

    def terminate(self) -> None:
        pass

    def kill(self) -> None:
        pass

    def wait(self, timeout: Optional[float] = None) -> int:
        return 12345


class SubprocessAppRun(AppRun):
    """
    Implements subprocess management for apps launched via Popen
    """

    _preamble_cache: Dict[str, Path] = {}

    def _build_cmdline(self) -> str:
        return ""

    def _build_preamble(self) -> str:
        if not self._preamble:
            return ""
        if isinstance(self._preamble, list):
            return " && ".join(self._preamble) + " && "

        if self._preamble not in self._preamble_cache:
            with tempfile.NamedTemporaryFile(mode="w", delete=False) as fp:
                fp.write(self._preamble)
                fp.flush()
                self._preamble_cache[self._preamble] = Path(fp.name).resolve()
        return f"source {self._preamble_cache[self._preamble]} && "

    def _set_envs(self) -> None:
        envs = os.environ.copy()
        envs.update(self._envs)
        # Check the assigned GPU ID list from the first compute node:
        gpu_ids = self._node_spec.gpu_ids[0]
        if gpu_ids:
            envs["CUDA_DEVICE_ORDER"] = "PCI_BUS_ID"
            envs["CUDA_VISIBLE_DEVICES"] = ",".join(map(str, gpu_ids))
        envs["OMP_NUM_THREADS"] = str(self._threads_per_rank)
        self._envs = envs

    def _open_outfile(self) -> IO[bytes]:
        return open(self._outfile_path, "wb")

    def _pre_popen(self) -> None:
        pass

    def _post_popen(self) -> None:
        pass

    def start(self) -> None:
        self._set_envs()
        cmdline = self._build_preamble() + self._build_cmdline()
        logger.info(f"{self.__class__.__name__} Popen: {cmdline}")
        self._outfile = self._open_outfile()
        self._pre_popen()

        try:
            self._process = subprocess.Popen(
                cmdline,
                shell=True,
                executable="/bin/bash",
                stdout=self._outfile,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                env=self._envs,
                cwd=self._cwd,
            )
        except Exception as e:
            logger.error(f"Popen failed: {e}")
            self._process = cast("subprocess.Popen[bytes]", FailedStartProcess())
        self._post_popen()

    def poll(self) -> Optional[int]:
        returncode = self._process.poll()
        if returncode is not None:
            self._outfile.close()
        return returncode

    def terminate(self) -> None:
        return self._process.terminate()

    def kill(self) -> None:
        self._process.kill()
        self._outfile.close()
        self._process.poll()

    def wait(self, timeout: Optional[float] = None) -> int:
        try:
            retcode = self._process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            cmdline = self._build_preamble() + self._build_cmdline()
            assert timeout is not None
            raise TimeoutExpired(cmd=cmdline, timeout=timeout)
        else:
            self._outfile.close()
            return retcode

    def tail_output(self, nlines: int = 10) -> str:
        return subprocess.check_output(
            ["tail", "-n", str(nlines), self._outfile_path],
            encoding="utf-8",
        )


class LocalAppRun(SubprocessAppRun):
    def _build_cmdline(self) -> str:
        return self._cmdline

    def _pre_popen(self) -> None:
        cpu_ids = self._node_spec.cpu_ids[0]
        if cpu_ids:
            _psutil_process.cpu_affinity(cpu_ids)

    def _post_popen(self) -> None:
        _psutil_process.cpu_affinity([])
